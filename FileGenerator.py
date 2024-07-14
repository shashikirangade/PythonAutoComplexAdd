import json
import random

# Generate Parent Records
parent_records = []
for i in range(1, 201):
    eval_parts = [f"row{random.randint(1, 500)}" for _ in range(4)]
    evaluation = f"{eval_parts[0]} OR {eval_parts[1]} AND ({eval_parts[2]} OR {eval_parts[3]})"
    parent_records.append({
        "source_id": i,
        "promo": f"promo{i}",
        "evaluation": evaluation
    })

# Write Parent Records to JSON
with open('parent.json', 'w') as f:
    json.dump(parent_records, f, indent=4)

# Generate Child Records
products = ['packimus', 'proximus', 'optimus', 'maximus']
operators = ['=', '<>', 'LIKE']
attribute_names = ['attribute1', 'attribute2', 'attribute3', 'attribute4']
attribute_types = ['type1', 'type2']

child_records = []
for i in range(1, 501):
    child_records.append({
        "id": i,
        "source_id": random.randint(1, 200),
        "operator": random.choice(operators),
        "product": random.choice(products),
        "condition_attribute_name": random.choice(attribute_names),
        "attribute_type": random.choice(attribute_types),
        "value": f"value{random.randint(1, 10)}"
    })

# Write Child Records to JSON
with open('child.json', 'w') as f:
    json.dump(child_records, f, indent=4)

print("Synthetic data generated successfully.")