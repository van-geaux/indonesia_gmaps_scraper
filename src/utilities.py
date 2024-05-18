import urllib.parse


def remove_spaces(input_string):
    result_string = input_string.replace(" ", "")
    return result_string

def create_search_link(query: str, lang, geo_coordinates, zoom):
    if geo_coordinates is None and zoom is not None:
        raise ValueError("geo_coordinates must be provided along with zoom")

    endpoint = urllib.parse.quote_plus(query)

    params = {'authuser': '0',
              'hl': lang,
              'entry': 'ttu',} if lang is not None else {'authuser': '0',
                                                         'entry': 'ttu',}
    
    geo_str = ''
    if geo_coordinates is not None:
        geo_coordinates = remove_spaces(geo_coordinates)
        if zoom is not None:
            geo_str = f'/@{geo_coordinates},{zoom}z'
        else:
            geo_str = f'/@{geo_coordinates}'

    url = f'https://www.google.com/maps/search/{endpoint}'
    if geo_str:
        url += geo_str
    url += f'?{urllib.parse.urlencode(params)}'

    return url

def clean_table_name(jenis, filter_wilayah=''):
    jenis_table = jenis.replace(' ', '_') + '_filtered' if filter_wilayah else jenis.replace(' ', '_')
    return jenis_table