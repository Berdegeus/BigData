from functools import reduce

# Step 1: Define the Product class
class Product:
    def __init__(self, description, price):
        self.description = description
        self.price = price

    def __repr__(self):
        return f"{self.description}: ${self.price:.2f}"

# Step 2: Create a list of Product objects
products = [
    Product("Chair", 500),
    Product("Mouse", 150),
    Product("Monitor", 1200)
]

# Step 3: Increase all products' prices by 10% using map
increased_prices = list(map(lambda p: Product(p.description, p.price * 1.1), products))

# Step 4: Filter products with price > 1000
expensive_products = list(filter(lambda p: p.price > 1000, increased_prices))

# Step 5: Sum all products' prices
total_price = reduce(lambda acc, p: acc + p.price, increased_prices, 0)

# Step 6: Find the product with the highest price
most_expensive_product = reduce(lambda p1, p2: p1 if p1.price > p2.price else p2, increased_prices)

# Display the results
print("Products with increased prices:")
print(increased_prices)
print("\nProducts with price > 1000:")
print(expensive_products)
print("\nTotal price of all products:")
print(total_price)
print("\nProduct with the highest price:")
print(most_expensive_product)
