
"""
Constants are given on this module
- to avoid circular imports
- to fascilitate editing German annotations (almost all of them are here)
"""

from collections import OrderedDict
from datetime import date
from dash import dcc
from dash.dash_table import DataTable


# Activate debugging mode?
DEBUG = False

# Custom prefix for error messages (for better debugging)
ERR_PREFIX = "BesserEsser:"

# Change the SQL driver name and the name of your schema as necessary
# HOST, PORT, DATABASE, USER, PASSWORD keywords (as they appear in your .env) are on data_access.py
DRIVER = 'postgresql'                 # maybe 'postgresql+psycopg2' or 'mysql'
SCHEMA = 'capstone_foodintolerance'   # maybe 'besseresser' ?

# TODO: Put names of the SQL database tables and column names here (to avoid hard-coding)
ACCOUNT_TABLE = "account"        # example
INGREDIENT_ID = "ingredient_id"  # not used at the moment
...

# Path to the banner image
BANNER_PATH = "assets/banner.png"

# default min/max dates for the dropdown calendar
MIN_DATE = date(2021, 1, 1)
MAX_DATE = date.today()

# Konto / Kunde / Nutzer?
ACCOUNT = "Konto"   # e.g. for the "Konto nicht gefunden" message

# Other language related annotations
TITLE_UNIT_0 = None   # the "Konto Suchen" section
TITLE_UNIT_1 = "Zeitraum"  # Zeitraum wählen
TITLE_UNIT_2 = "Eckdaten"  #Statistics / Usage overview
TITLE_UNIT_3 = "Ernährungs- und Symptomtagebuch"   #Mahlzeiten und Beschwerden

TITLE_UNIT_4 = "Welche Lebensmittel werden am meisten konsumiert"   # Meist konsumierte Lebensmittel
TITLE_UNIT_4 = "Meist konsumierte Lebensmittel"

TITLE_UNIT_5 = "Welche Lebensmittel wurden unmittelbar vor der Symptomentstehung gegessen?" #
TITLE_UNIT_5 = "Konsumierte Lebensmittel vor dokumentiertem Symptom"

TITLE_UNIT_6 = "Wie sieht ein typisches Frühstück, Mittagessen oder Abendessen aus?"
TITLE_UNIT_6 = "Typische Mahlzeiten zu verschieden Tageszeiten"

TITLE_UNIT_7 = "Welche Lebensmittel werden (in einer bestimmten Mahlzeit) kombiniert?"
TITLE_UNIT_7 = "Kombinierte Lebensmittel zu verschiedenen Tageszeiten"

TITLE_UNIT_8 = "Potentiell Symptom verursachende Lebensmittel"

TEXT_UNIT_0 = "Suchen"
TEXT_UNIT_1 = "Zeitraum wählen (Es wird der zuletzt dokumentierte Zeitraum angezeigt)"
TEXT_UNIT_2 = None
TEXT_UNIT_3 = "Tage mit Symptomen"  # located on plotting_functions.py
TEXT_UNIT_4 = None  # the values of the dropdown menu and radio buttons are hard-coded on units.py
TEXT_UNIT_5 = "Beinträchtigungsgrad (die angegebene Zahl und höher)"
TEXT_UNIT_6 = None
TEXT_UNIT_7 = "Anzahl der Komponenten"
TEXT_FOOTER = "BesserEsser"

# Mapping for meals: ENGLISH - German (as they appear in the database)
# used for selectors
MEALS_MAPPING = OrderedDict([("BREAKFAST", "Frühstück"), 
                             ("LUNCH", "Mittagessen"), 
                             ("DINNER", "Abendessen")])

# Generic constants for all selectors' values
# These generic values are used for all types of selectors to avoid hard-coding
A = 'A'   # e.g. for "Alle Mahlzetien"   (A is the default value for all selectors)
B = 'B'   # e.g. for BREAKFAST
C = 'C'   # e.g. for LUNCH
D = 'D'   # e.g. for DINNER
E = 'E'   # extend when needed
...


# plotly dash component classes (for type comparison)
DASH_COMPONENTS_CLASSES = (dcc.Dropdown, dcc.RadioItems, dcc.Slider,
                           dcc.DatePickerRange, dcc.DatePickerSingle, dcc.Graph, DataTable)

# Some colors for future styling
# Keep in mind - there are 3 types of background colors:
WEBPAGE_BACKGROUND_COLOR = "white"
GRAPH_MARGINS_COLOR = "white"         # used for the no_data plot
GRAPH_PLOTTING_AREA_COLOR = "white"   # used for the no_data plot
...

# Some basic styling
css = {
        "color": "black",
        "backgroundColor": "white",
        "fontSize": 16,
        "fontFamily":"Arial",  # PT Sans ?
        "text-align": "left"
            }