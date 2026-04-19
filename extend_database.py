"""
BUTTERNUT BOX — Extended Tables Generator
==========================================
Adds recipes, order_items, and pet_profiles to existing database.
Run this after setup.py
"""

import duckdb
import random
from datetime import date

random.seed(99)

conn = duckdb.connect("butternutbox.db")

print("Adding new tables...")

conn.execute("""
    DROP TABLE IF EXISTS order_items;
    DROP TABLE IF EXISTS recipes;
    DROP TABLE IF EXISTS pet_profiles;
""")

# ── RECIPES TABLE ─────────────────────────────────────────────────────────────
conn.execute("""
    CREATE TABLE recipes (
        recipe_id       INTEGER PRIMARY KEY,
        recipe_name     VARCHAR,
        protein_type    VARCHAR,
        cost_per_pouch  DECIMAL(4,2),
        is_premium      BOOLEAN
    )
""")

conn.execute("""
    INSERT INTO recipes VALUES
    (1,  'Chicken You Out',          'Chicken', 1.20, false),
    (2,  'Beef It Up',               'Beef',    1.45, false),
    (3,  'Wham Bam Lamb',            'Lamb',    1.55, false),
    (4,  'Pork This Way',            'Pork',    1.25, false),
    (5,  'Duo of Duck & Chicken',    'Duck',    1.65, true),
    (6,  'Salmon To Love',           'Salmon',  1.80, true),
    (7,  'Gobble Gobble Turkey',     'Turkey',  1.40, false),
    (8,  'You''ve Got Game',         'Game',    1.90, true),
    (9,  'Duo of Chicken & Salmon',  'Salmon',  1.70, true)
""")

print("✅ Recipes loaded — 9 recipes")

# ── PET PROFILES TABLE ────────────────────────────────────────────────────────
conn.execute("""
    CREATE TABLE pet_profiles (
        customer_id         INTEGER PRIMARY KEY REFERENCES customers(customer_id),
        weight_kg           DECIMAL(4,1),
        body_condition      VARCHAR,
        activity_level      VARCHAR,
        health_condition    VARCHAR,
        previous_food_type  VARCHAR
    )
""")

# Realistic pet profiles matched to dog sizes
# small = 3-12kg, medium = 12-25kg, large = 25-45kg
body_conditions  = ['underweight', 'ideal', 'ideal', 'ideal', 'overweight']
activity_levels  = ['low', 'moderate', 'moderate', 'high']
health_conditions = ['none', 'none', 'none', 'sensitive stomach', 'joint problems',
                     'weight management', 'skin issues']
food_types = ['dry kibble', 'dry kibble', 'wet food', 'raw', 'mixed']

# Churned customers (ids) - give them slightly worse health profiles
churned_ids = {3,7,11,15,19,22,28,32,35,38,39}

profiles = []
for i in range(1, 41):
    if i <= 12:    # small dogs
        weight = round(random.uniform(3.0, 12.0), 1)
    elif i <= 26:  # medium dogs
        weight = round(random.uniform(12.0, 25.0), 1)
    else:          # large dogs
        weight = round(random.uniform(25.0, 45.0), 1)

    # Churned customers slightly more likely to be overweight or have no condition
    if i in churned_ids:
        body = random.choice(['ideal', 'overweight', 'overweight'])
        health = random.choice(['none', 'none', 'none', 'weight management'])
        prev_food = random.choice(['dry kibble', 'dry kibble', 'mixed'])
    else:
        body = random.choice(body_conditions)
        health = random.choice(health_conditions)
        prev_food = random.choice(food_types)

    activity = random.choice(activity_levels)
    profiles.append((i, weight, body, activity, health, prev_food))

conn.executemany("""
    INSERT INTO pet_profiles VALUES (?, ?, ?, ?, ?, ?)
""", profiles)

print(f"✅ Pet profiles loaded — {len(profiles)} profiles")

# ── ORDER ITEMS TABLE ─────────────────────────────────────────────────────────
conn.execute("""
    CREATE TABLE order_items (
        item_id         INTEGER PRIMARY KEY,
        order_id        INTEGER REFERENCES orders(order_id),
        recipe_id       INTEGER REFERENCES recipes(recipe_id),
        pouches         INTEGER,
        cost_price      DECIMAL(6,2)
    )
""")

# Fetch all orders with customer info
orders = conn.execute("""
    SELECT o.order_id, o.subscription_id, s.customer_id,
           c.dog_size, o.order_date,
           s.status, c.signup_date
    FROM orders o
    JOIN subscriptions s ON o.subscription_id = s.subscription_id
    JOIN customers c ON s.customer_id = c.customer_id
    ORDER BY o.order_id
""").fetchall()

# Pouches per dog size
pouches_map = {'small': 8, 'medium': 14, 'large': 20}

# Recipe preferences per customer - some customers are engaged (rotate recipes)
# others are disengaged (same recipe every month)
# Churned customers tend to be disengaged - pick 1-2 recipes and stick to them
# Retained customers rotate more

customer_recipe_prefs = {}
for cid in range(1, 41):
    if cid in churned_ids:
        # Disengaged — pick 1 or 2 recipes and never change
        base = random.sample(range(1, 10), random.choice([1, 2]))
        customer_recipe_prefs[cid] = {'engaged': False, 'recipes': base}
    else:
        # Engaged — has a preference pool of 3-5 recipes they rotate through
        base = random.sample(range(1, 10), random.choice([3, 4, 5]))
        customer_recipe_prefs[cid] = {'engaged': True, 'recipes': base}

item_id = 1
items = []

for order in orders:
    order_id, sub_id, cid, dog_size, order_date, status, signup_date = order
    prefs = customer_recipe_prefs[cid]
    pouches = pouches_map[dog_size]
    pool = prefs['recipes']

    # Engaged customers: rotate recipe every 1-2 months
    # Disengaged: same recipe repeatedly
    if prefs['engaged']:
        # Pick a recipe based on order position — rotate through pool
        order_month_num = (order_date.year - 2023) * 12 + order_date.month
        recipe_id = pool[order_month_num % len(pool)]
    else:
        # Always pick the same one
        recipe_id = pool[0]

    cost = conn.execute(
        "SELECT cost_per_pouch FROM recipes WHERE recipe_id = ?", 
        [recipe_id]
    ).fetchone()[0]

    total_cost = round(float(cost) * pouches, 2)
    items.append((item_id, order_id, recipe_id, pouches, total_cost))
    item_id += 1

conn.executemany("""
    INSERT INTO order_items VALUES (?, ?, ?, ?, ?)
""", items)

print(f"✅ Order items loaded — {len(items)} items")

# ── VERIFY ────────────────────────────────────────────────────────────────────
result = conn.execute("""
    SELECT
        (SELECT COUNT(*) FROM recipes)     AS recipes,
        (SELECT COUNT(*) FROM pet_profiles) AS pet_profiles,
        (SELECT COUNT(*) FROM order_items)  AS order_items
""").fetchone()

print(f"\n📊 New tables:")
print(f"   recipes: {result[0]} | pet_profiles: {result[1]} | order_items: {result[2]}")
print("\n✅ Extended database ready.")
conn.close()