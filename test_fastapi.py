"""
Test suite for the enhanced single-table NL2SQL system.
Tests against the employees table with rich metadata.
"""
import json
from nlp.parser import NLParser
from nlp.entity_extractor import EntityExtractor, IntentClassifier
from query_builder.sql_generator import SQLGenerator

# Rich metadata with data types, descriptions, sample values, distinct values
meta = {
    "tables": {
        "employees": {
            "description": "Contains employee records",
            "columns": {
                "employee_id": {
                    "data_type": "integer",
                    "description": "Unique employee identifier",
                    "is_primary_key": True,
                    "sample_values": [1, 2, 3]
                },
                "first_name": {
                    "data_type": "text",
                    "description": "Employee first name",
                    "sample_values": ["John", "Jane", "Priya"],
                    "distinct_values": ["John", "Jane", "Priya", "Raj", "Alice"]
                },
                "last_name": {
                    "data_type": "text",
                    "description": "Employee last name",
                    "sample_values": ["Doe", "Smith", "Kumar"]
                },
                "email": {
                    "data_type": "text",
                    "description": "Employee email address",
                    "sample_values": ["john@company.com"]
                },
                "department": {
                    "data_type": "text",
                    "description": "Department the employee belongs to",
                    "sample_values": ["IT", "HR", "Finance", "Marketing", "Sales"],
                    "distinct_values": ["IT", "HR", "Finance", "Marketing", "Sales"]
                },
                "salary": {
                    "data_type": "numeric",
                    "description": "Employee salary",
                    "sample_values": [50000, 75000, 60000]
                },
                "city": {
                    "data_type": "text",
                    "description": "City where the employee lives",
                    "sample_values": ["Chennai", "Mumbai", "Delhi"],
                    "distinct_values": ["Chennai", "Mumbai", "Delhi", "Bangalore", "New York"]
                },
                "gender": {
                    "data_type": "text",
                    "description": "Employee gender",
                    "sample_values": ["Male", "Female"],
                    "distinct_values": ["Male", "Female"]
                },
                "hire_date": {
                    "data_type": "date",
                    "description": "Date when the employee was hired",
                    "sample_values": ["2020-01-15", "2019-06-01"]
                },
                "job_title": {
                    "data_type": "text",
                    "description": "Employee job title",
                    "sample_values": ["Engineer", "Manager", "Analyst"],
                    "distinct_values": ["Engineer", "Manager", "Analyst", "Designer", "Director"]
                },
                "country": {
                    "data_type": "text",
                    "description": "Country of residence",
                    "sample_values": ["India", "USA", "UK"],
                    "distinct_values": ["India", "USA", "UK", "Germany"]
                }
            }
        }
    }
}

print("Loading NL2SQL components...")
parser = NLParser()
classifier = IntentClassifier()

tests = [
    # Basic queries
    "Show all employees",
    "List employees from Chennai",
    "Show employees in the IT department",
    "Show male employees",

    # Aggregations
    "How many employees are there",
    "What is the average salary",
    "Find the highest salary",
    "Total number of employees per department",
    "Average salary per department",
    "Count of employees in each city",

    # Filters
    "Employees with salary greater than 50000",
    "Show employees with salary between 40000 and 80000",
    "List female employees from Mumbai",

    # Ordering + Limit
    "Top 5 employees by salary",
    "Show the employee with the highest salary",
    "List employees ordered by salary ascending",
    "list employee hired after 2020",

    # Non-matching
    "What is the capital of France?",
]

passed = 0
failed = 0

for q in tests:
    print("=" * 60)
    print(f"  Query: {q}")
    try:
        extractor = EntityExtractor(meta)
        generator = SQLGenerator(meta)

        analysis = parser.get_analysis(q)
        entities = extractor.extract(analysis, q)
        intent   = classifier.classify(q, analysis)

        if not entities.get("table"):
            print(f"  Result: NOT MATCHING (no table found)")
        else:
            sql = generator.generate(entities, intent)
            print(f"  SQL:    {sql}")
        passed += 1
    except Exception as e:
        print(f"  ERROR:  {e}")
        import traceback
        traceback.print_exc()
        failed += 1

print("=" * 60)
print(f"\n✅ Passed: {passed}  |  ❌ Failed: {failed}")
