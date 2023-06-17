import redis
import json
from collections import namedtuple

class Order:
    def __init__(self, order_id, side, symbol, price, quantity, filled_quantity=0,
                 #  created_at=datetime.datetime.now().timestamp()
                 ):
        self.order_id = order_id
        self.side = side
        self.symbol = symbol
        self.price = price
        self.quantity = quantity
        self.filled_quantity = filled_quantity
        self.status = 'open'
        # self.created_at = str(created_at)

    def as_dict(self, ):
        return dict(
            order_id=self.order_id,
            side=self.side,
            symbol=self.symbol,
            price=self.price,
            quantity=self.quantity,
            filled_quantity=self.filled_quantity,
            status=self.status
        )

    def as_json(self):
        struct = namedtuple('as_json', 'order_id side symbol price quantity filled_quantity status')
        as_json = struct(
            order_id=self.order_id,
            side=self.side,
            symbol=self.symbol,
            price=self.price,
            quantity=self.quantity,
            filled_quantity=self.filled_quantity,
            status=self.status
        )
        return as_json


class MatchEngine:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)

    def add_order(self, order_id, side, symbol, price, quantity):
        self.order_id = order_id
        order = Order(self.order_id, side, symbol, price, quantity)
        order_key = f'order:{self.order_id}:{price}'
        order_data = {
            'order_id': order.order_id,
            'side': order.side,
            'symbol': order.symbol,
            'price': order.price,
            'quantity': order.quantity,
            'filled_quantity': order.filled_quantity,
            'status': order.status,
        }
        self.redis.hset(order_key, mapping=order_data)
        if order.side == 'buy':
            self.redis.rpush(f'buy_orders:{order.symbol}', order_key)
        else:
            self.redis.rpush(f'sell_orders:{order.symbol}', order_key)
        market_key = f'{symbol}-market'
        market_volume_data = self.redis.hgetall(market_key)
        if market_volume_data:
            market_volume_data[f'{side}_orders_quantity'] = float(market_volume_data[f'{side}_orders_quantity']) + quantity
            market_volume_data[f'{side}_orders_amount'] = float(market_volume_data[f'{side}_orders_amount']) + quantity * price
            self.redis.hset(market_key, mapping=market_volume_data)
        else:

            market_data = {"buy_orders_quantity": 0, "buy_orders_amount": 0, "sell_orders_quantity": 0, "sell_orders_amount": 0, f'{side}_orders_quantity': quantity, f'{side}_orders_amount': quantity * price}
            self.redis.hset(market_key, mapping=market_data)
        print('11111111')
        self.redis.rpush(f'{order.symbol}-taken-orders', order_key)
        self.match_orders(order)

    def match_orders(self, new_order):

        if new_order.side == 'buy':
            orders_to_match = [(int(order.split(':')[1]), int(order.split(':')[2])) for order in self.redis.lrange(f'sell_orders:{new_order.symbol}', 0, -1)]
            orders_to_match.sort(key=lambda order: (-order[1], order[0]))
            # orders_to_match = sorted(orders_to_match,key=lambda x: (float(x.split(':')[2]),x.split(':')[1]))
        else:
            orders_to_match = [(int(order.split(':')[1]), int(order.split(':')[2])) for order in self.redis.lrange(f'buy_orders:{new_order.symbol}', 0, -1)]
            orders_to_match.sort(key=lambda order: (order[1], order[0]))
            # orders_to_match = sorted(orders_to_match,key=lambda x: (float(x.split(':')[2]),x.split(':')[1]),reverse=True)
        orders_to_match = [f'order:{order[0]}:{order[1]}' for order in orders_to_match]
        matched_orders = []
        trades = []
        for order_key in orders_to_match:
            order_data = self.redis.hgetall(order_key)
            order = Order(
                order_data['order_id'], order_data['side'],
                order_data['symbol'], float(order_data['price']), float(order_data['quantity']), float(order_data['filled_quantity'])
            )
            price_condition = order.price < new_order.price if new_order.side == 'sell' else order.price > new_order.price
            if order.status == 'open' and not price_condition:
                taken_orders = self.redis.lrange(f'{order.symbol}-taken-orders', 0, -1)
                print('taken-orders',taken_orders)
                if order_key in taken_orders:
                    print('injaaaaaaaaaaaa')
                    continue
                self.redis.rpush(f'{order.symbol}-taken-orders', order_key)
                trade_quantity = min(order.quantity - order.filled_quantity, new_order.quantity - new_order.filled_quantity)
                if trade_quantity > 0:
                    trade_price = order.price
                    trade_data = {
                        'buy_order_id': new_order.order_id if new_order.side == 'buy' else order.order_id,
                        'sell_order_id': order.order_id if new_order.side == 'buy' else new_order.order_id,
                        'price': trade_price,
                        'quantity': trade_quantity
                    }
                    self.redis.rpush(f'trades:{new_order.symbol}', json.dumps(trade_data))
                    new_order.filled_quantity += trade_quantity
                    order.filled_quantity += trade_quantity
                    # self.update_wallets(order, new_order, trade_quantity, trade_price)

                    order.status = 'filled' if order.filled_quantity == order.quantity else 'open'

                    self.redis.hset(order_key, mapping={'status': order.status, 'filled_quantity': order.filled_quantity})
                    if order.status == 'filled':
                        self.redis.lrem(f'{order.side}_orders:{order.symbol}', 1, order_key)
                    # user later maybe
                    # self.redis.hdel(f'order:{order.order_id}',*self.redis.hkeys(f'order:{order.order_id}'))
                    matched_orders.append(order.as_dict())
                    trades.append(trade_data)
                    new_order.status = 'filled' if new_order.filled_quantity == new_order.quantity else 'open'
                    self.redis.hset(f'order:{new_order.order_id}:{new_order.price}', mapping={'status': new_order.status, 'filled_quantity': new_order.filled_quantity})
                    if new_order.status == 'filled':
                        self.redis.lrem(f'{new_order.side}_orders:{order.symbol}', 1, f'order:{new_order.order_id}:{new_order.price}')
                        self.redis.lrem(f'{order.symbol}-taken-orders', 1, order_key)
                        break
                    # use later maybe
                    # self.redis.hdel(f'order:{new_order.order_id}',*self.redis.hkeys(f'order:{new_order.order_id}'))
                    # break

                self.redis.lrem(f'{order.symbol}-taken-orders', 1, order_key)
        print('2222222')
        self.redis.lrem(f'{new_order.symbol}-taken-orders', 1, f'order:{new_order.order_id}:{new_order.price}')
        matched_orders.append(new_order.as_dict())
        return matched_orders, trades
    

engine = MatchEngine()

import random
side = {
    1:"buy",
    2:"sell"
}
import time
start_time = time.time()

market = {
    1:"btc",
    2:"eth",
    3:"bnb",
    4:"shib",
    5:"sushi",
}


import asyncio
def btc_market(i):
    engine = MatchEngine()

    # for i in range(1000):
    engine.add_order(i,side[random.randint(1,2)],'btc-usdt',random.randint(20000,21000),random.uniform(0.001, 0.003))
async def bnb_market():
    for i in range(1000,2000):
        engine.add_order(i,side[random.randint(1,2)],'btc-usdt',random.randint(20000,21000),random.uniform(0.001, 0.003))

async def eth_market():
    for i in range(2000,3000):
        engine.add_order(i,side[random.randint(1,2)],'btc-usdt',random.randint(20000,21000),random.uniform(0.001, 0.003))


async def shib_market():
    for i in range(3000,4000):
        engine.add_order(i,side[random.randint(1,2)],'shib-usdt',random.randint(20000,21000),random.uniform(0.001, 0.003))

async def sushi_market():
    for i in range(4000,8000):
        engine.add_order(i,side[random.randint(1,2)],'sushi-usdt',random.randint(20000,21000),random.uniform(0.001, 0.003))

a = [f for f in range(8000)]
from multiprocessing import Pool

def hand():
    p = Pool(5)
    p.map(btc_market, a)

if __name__ == '__main__':
    hand()

async def r():
    l = await asyncio.gather(
        btc_market(),
        eth_market(),
        bnb_market(),
        # shib_market(),
        # sushi_market()
        )
        


# if __name__ == "__main__":
#     asyncio.run(r())
t = time.time() - start_time

red = redis.Redis(host="localhost", port=6379, db=0,decode_responses=True)

# print(red.lrange(f'buy_orders:btc-usdt', 0, -1))

for h in red.lrange(f'sell_orders:btc-usdt', 0, -1):
    print(red.hgetall(h),'seeeeeelllllllss')
print('==================\n')
for j in red.lrange(f'buy_orders:btc-usdt', 0, -1):
    print(red.hgetall(j),'bbbuuuuuuuuuuys')





print("--- %s seconds ---" % (t))

print('each order',t/8000)
print(len(red.lrange(f'trades:btc-usdt', 0, -1)))


# for t in red.lrange(f'trades:btc-usdt', 0, -1):
#     print(t,'traaaaaaades')
print(len(red.lrange(f'trades:bnb-usdt', 0, -1)))
print(len(red.lrange(f'trades:eth-usdt', 0, -1)))
print(len(red.lrange(f'trades:shib-usdt', 0, -1)))
print(len(red.lrange(f'trades:sushi-usdt', 0, -1)))
