import json
import logging
from nlp.parser import NLParser
from nlp.entity_extractor import EntityExtractor, IntentClassifier
from query_builder.sql_generator import SQLGenerator

logging.basicConfig(level=logging.INFO)

meta = {
  'tables': {
    'customers': {
      'columns': ['customer_id', 'first_name', 'last_name', 'email', 'phone', 'city', 'created_at'],
      'primary_key': 'customer_id',
      'foreign_keys': {}
    },
    'order_items': {
      'columns': ['order_item_id', 'order_id', 'product_id', 'quantity', 'price'],
      'primary_key': 'order_item_id',
      'foreign_keys': {
        'order_id': 'orders.order_id',
        'product_id': 'products.product_id'
      }
    },
    'orders': {
      'columns': ['order_id', 'customer_id', 'rep_id', 'order_date', 'total_amount'],
      'primary_key': 'order_id',
      'foreign_keys': {
        'customer_id': 'customers.customer_id',
        'rep_id': 'sales_reps.rep_id'
      }
    },
    'products': {
      'columns': ['product_id', 'product_name', 'category', 'price', 'stock_quantity', 'created_at'],
      'primary_key': 'product_id',
      'foreign_keys': {}
    },
    'sales_reps': {
      'columns': ['rep_id', 'rep_name', 'email', 'region', 'hire_date'],
      'primary_key': 'rep_id',
      'foreign_keys': {}
    }
  }
}

print("Loading ML models...")
parser = NLParser()
classifier = IntentClassifier()

tests = [
    'Show all customers from the city of Chennai',
    'Find the total quantity of products sold in each category',
    'Find the sales representative handling the highest total amount of orders',
    'List products ordered by price highest first',
    'What is the capital of Japan?'
]

for q in tests:
    print('--------------------------------')
    print(f'User Query: {q}')
    try:
        extractor = EntityExtractor(meta)
        generator = SQLGenerator(meta)
        
        print("Parsing...")
        analysis = parser.get_analysis(q)
        print("Extracting...")
        entities = extractor.extract(analysis, q)
        print("Classifying...")
        intent = classifier.classify(q, analysis)
        
        if not entities.get("tables"):
            print("Is Matching: False")
        else:
            print("Generating...")
            sql = generator.generate(entities, intent, extractor.graph)
            print(f"Is Matching: True")
            print(f"SQL: {sql}")
    except Exception as e:
        print(f"FAILED TO EXECUTE: {e}")
