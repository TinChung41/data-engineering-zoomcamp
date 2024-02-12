if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Function to convert CamelCase to snake_case
def camel_to_snake(name):
    import re
    str1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', str1).lower()

@transformer
def transform(data, *args, **kwargs):
    # Rename the columns
    data.columns = [camel_to_snake(col) for col in data.columns]
    
    #new column 
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    # #remove records with 0 passenger_count & 0 trip_distance
    filtered_data = data[(data['passenger_count'] > 0) \
                        &(data['trip_distance']   > 0)
                        ]
    
    return filtered_data


@test
def test_output(output, *args) -> None:
    assert 'vendor_id' in output.columns, "output does not have column: 'vendor_id '"
    assert output['passenger_count'].isin([0]).sum() ==0, 'output has records with 0 passenger_count'
    assert output['trip_distance'].isin([0]).sum() ==0, 'output has records with 0 trip_distance'
    