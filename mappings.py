gas = {}


def map_kwargs(feature_name) -> dict:
    if feature_name == 'river_level':
        return {'mode': 'distance'}
    elif feature_name == 'precipitation':
        return {'mode': feature_name, 'interval': 3600, 'view_type': 1}
    else:
        return {'mode': feature_name}
