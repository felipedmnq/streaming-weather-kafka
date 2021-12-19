from configparser import ConfigParser

def config(filename='weather_api_key.ini', section='openweather'):
    '''Get API key

    Params:
    -------
        filename - str: .INI file with API key stored.
        section - str: section name.
    
    Retunr:
    -------
        str: API key    
    '''
    # create a parser
    parser = ConfigParser()
    # read API key file
    parser.read(filename)
    # check if section exists
    if parser.has_section(section):
        params = parser.items(section)
        return params[0][1]
    else:
        raise Exception(f'Section {section} not found in {filename}')

    