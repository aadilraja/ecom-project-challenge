import csv, random, datetime
from pathlib import Path

root = Path('C:/coding/ecom-project')
random.seed(42)

start_date = datetime.date(2023, 1, 1)
end_date = datetime.date(2024, 12, 31)

def random_date():
    delta = (end_date - start_date).days
    return start_date + datetime.timedelta(days=random.randint(0, delta))

category_names = [
    'Electronics', 'Books', 'Clothing', 'Home Decor', 'Beauty',
    'Sports', 'Toys', 'Groceries', 'Garden', 'Automotive'
]
categories = [
    {'category_id': idx + 1, 'category_name': name}
    for idx, name in enumerate(category_names)
]
with (root / 'categories.csv').open('w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['category_id', 'category_name'])
    writer.writeheader()
    writer.writerows(categories)

users = []
for user_id in range(1, 101):
    username = f'user{user_id:03d}'
    email = f'{username}@example.com'
    created_at = random_date().isoformat()
    users.append({'user_id': user_id, 'username': username, 'email': email, 'created_at': created_at})
with (root / 'users.csv').open('w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['user_id', 'username', 'email', 'created_at'])
    writer.writeheader()
    writer.writerows(users)

products = []
for product_id in range(1, 201):
    product_name = f'Product {product_id:03d}'
    category_id = random.choice(categories)['category_id']
    price = round(random.uniform(5.0, 500.0), 2)
    products.append({'product_id': product_id, 'product_name': product_name, 'category_id': category_id, 'price': f'{price:.2f}'})
with (root / 'products.csv').open('w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['product_id', 'product_name', 'category_id', 'price'])
    writer.writeheader()
    writer.writerows(products)

order_statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
orders = []
for order_id in range(1, 501):
    user_id = random.choice(users)['user_id']
    order_date = random_date().isoformat()
    status = random.choice(order_statuses)
    orders.append({'order_id': order_id, 'user_id': user_id, 'order_date': order_date, 'status': status})
with (root / 'orders.csv').open('w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['order_id', 'user_id', 'order_date', 'status'])
    writer.writeheader()
    writer.writerows(orders)

order_items = []
for item_id in range(1, 1501):
    order_id = random.choice(orders)['order_id']
    product_id = random.choice(products)['product_id']
    quantity = random.randint(1, 5)
    order_items.append({'item_id': item_id, 'order_id': order_id, 'product_id': product_id, 'quantity': quantity})
with (root / 'order_items.csv').open('w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['item_id', 'order_id', 'product_id', 'quantity'])
    writer.writeheader()
    writer.writerows(order_items)

print('Synthetic CSV files generated in', root)
