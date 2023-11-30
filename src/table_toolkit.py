

"""
table generation and conversion functions for table generation
"""


from collections import OrderedDict
import pandas as pd
from io import StringIO
from dash import html
from dash.dash_table import DataTable

from constants import DEBUG, MEALS_MAPPING, DISPLAYNAME



def read_json(data):
    """
    Reads data from user's browser session and returns at as a pandas df.
    ‘data‘ is expected to be a jsonified df as str.
    To avoid a Warning from pandas ‘data‘ is wrapped in StringIO().
    The reason why this functionality is abstracted away in a separate function is to
    abstract away the behavior of pd.read_json, which may possibly change with time.

    Args:
        data: jsonified pandas.DataFrame as str
    
    Returns:
        a pandas.DataFrame object

    Raises:
        todo: raise TypeError if data is None (for example), etc.
    """
    return pd.read_json(StringIO(data), orient='split')



def to_json(df):
    """
    is here for the maintenance, 
    if pandas.DataFrame.to_json changes -> corrrect it here
    """
    return df.to_json(date_format='iso', orient='split')



def to_list_of_dicts(df):
    """
    Turns a df into a list of dictionaries - the suitable format
    to be returned by a callback function and as an input into 
    a plotly-dash DataTable
    """
    return df.to_dict(orient='records')



def instantiate_debug_table(id=None):
    """
    Returns instances of dash data frame (for debugging purposes)
    it is the table you see underneath each unit in DEBUG mode
    """
    PAGE_SIZE = 10
    return html.Div(style={'display': 'block' if DEBUG else 'none'},
                    children=[html.H4("data table for debugging:"),
                              DataTable(page_size=PAGE_SIZE, id=id) if id else DataTable(page_size=PAGE_SIZE)])



def make_diary_table(df_eating, df_symptomreport):
    """
    TODO
    """

    # Column names
    DATE = 'date'
    TIME = 'time'
    DAYTIME = 'daytime'
    SYMPTOM = 'symptom'  # placeholder, for now: "-"
    IMPAIRMENT = 'impairment'
    TEMP = 'sorting'
    UNKNOWN = "Unbekannt"
    AFTER_GETTING_UP = "Nach dem Aufstehen"

    MAPPING = OrderedDict([
        ('AFTER_GETTING_UP', 'GETTING_UP'),
        ('AFTER_BREAKFAST', 'BREAKFAST' ),
        ('AFTER_LUNCH', 'LUNCH'),
        ('AFTER_DINNER', 'DINNER'),
        ('UNKNOWN', 'UNKNOWN')
    ])

    # try these functions to make the table pretty (ingredients list)
    func_to_aggregate_strings = ", ".join   # try: list, tuple, set, "\n".join , ", ".join  to push it into plotly-datatable and get beautiful repr
    df_eating_agg = df_eating[[DATE, DAYTIME, DISPLAYNAME]].groupby([DATE, DAYTIME]).agg(func_to_aggregate_strings).reset_index()   # ", ".join

    df_symptomreport[DAYTIME] = df_symptomreport[TIME].map(MAPPING)
    df_symptomreport_agg = df_symptomreport[[DATE, DAYTIME, SYMPTOM, IMPAIRMENT]].groupby([DATE, DAYTIME]).agg({SYMPTOM: ", ".join, IMPAIRMENT: 'mean'}).reset_index()

    values_list = list(MAPPING.values()) + list(set(df_symptomreport_agg[DAYTIME].unique()).difference(set(MAPPING.values()))) # to be on the safe side
    
    df_diary = df_symptomreport_agg.merge(df_eating_agg, how='outer', on=[DATE, DAYTIME])
    df_diary[TEMP] = df_diary[DAYTIME].apply(lambda v: values_list.index(v))
    df_diary = df_diary.sort_values([DATE, TEMP]).drop(TEMP, axis=1)

    df_diary = df_diary.reindex(columns=[DATE, DAYTIME, DISPLAYNAME, SYMPTOM, IMPAIRMENT])

    mapping = {k:v for k,v in 
               zip( MAPPING.values(), 
                    ([AFTER_GETTING_UP] + list(MEALS_MAPPING.values()) + [UNKNOWN]))}

    df_diary[DAYTIME] = df_diary[DAYTIME].map(mapping)
    df_diary[IMPAIRMENT] = df_diary[IMPAIRMENT].apply(lambda v: str(round(v)) if not pd.isnull(v) else '')
    df_diary.reset_index(inplace=True, drop=True)
    return df_diary



def prettify_diary_table(df_diary):
    """
    workaround to make this table look nicer
    if put into the `make_diary_table` -> doesnt work
    """
    DATE = 'date'
    df_diary[DATE] = df_diary[DATE].dt.strftime(r'%d/%m/%Y')
    df_diary.loc[df_diary[DATE].duplicated(), DATE] = ''
    df_diary[DATE] = df_diary[DATE].astype(str)
    df_diary.columns = ["Datum", "Zeit", "Lebensmittel", "Symptome", "Beeinträchtigungsgrad"]
    return df_diary



def make_probably_bad_foods_table(df):
    """
    TODO: docs, describe the logic of the approach

    "probably bad foods" = the ingredients that probably cause the symptoms (in a given user)

    The idea behind this:
    Make a list of the ingredients that were eaten on the days when the user had a symptom, 
    but which were not eaten on the days without symptoms,
    and rank this list by the number of times when the user had a symptom on the days that ingredient was eaten.

    Use set theory operations:
      {set of all ingredients consumed on the days with a symptom}
    - {set of the ingredients consumed on the days without a symptom}  # i.e. set diff
    = {set of ingredients that probably cause the symptom}
    
    the ingredients in the resultant set are then ranked based on the
    number of days on which a given ingredient was eaten AND a symptom occurred.

    TODO: consider the column containing the impairment grade OR symptom_same_day 
                                    (if no data on impairment grade for that date)

    Returns:
        a pandas dataframe with one column
    """

    # Top n of "potentially bad" ingredients
    TOP_N = 5
    
    set_true = set(df.loc[df['symptom_same_day']==True, DISPLAYNAME])   # yes symptom on that day
    set_false = set(df.loc[df['symptom_same_day']==False, DISPLAYNAME]) # no symptom on that day
    set_potentially_bad = set_true.difference(set_false)

    # make a ranking
    sr = (df[[DISPLAYNAME, 'symptom_same_day', 'symptom_next_day']]
     .groupby(DISPLAYNAME).sum()
     .sort_values(['symptom_same_day', 'symptom_next_day'], ascending=[False, False])
     .sum(axis=1).replace({0:None}).dropna().reset_index()[DISPLAYNAME])
    
    ranking = {ingredient: rank for rank, ingredient in sr.items()}

    return (pd.DataFrame(set_potentially_bad or set_true)  # if set_potentially_bad is empty
            .assign(ranking=lambda df: df.squeeze().map(ranking)).dropna()
            .sort_values("ranking", ascending=True).head(TOP_N)
            .drop("ranking", axis=1)).reset_index(drop=True).rename({0:''}, axis=1)




def make_statistics_table(df_eating, df_symptomreport):
    """
    TODO
    """
    # Calculations
    #total number of symptoms
    symptom_count = df_symptomreport['date'].count()
    #total number of days with symptoms
    symptom_days = df_symptomreport['date'].nunique()
    #days of usage
    df = df_eating.merge(df_symptomreport, on= 'date', how= 'outer')
    usage_days= df['date'].nunique()
    #relation days with symptoms/days of usage
    symptom_days_perc= round(symptom_days*100/usage_days,1)
    #avg number of days with symptoms per week of usage
    df_symptomreport["week_no"]=pd.to_datetime(df_symptomreport['date']).dt.isocalendar().week
    symptoms= df_symptomreport.groupby('week_no')['date'].nunique().to_frame().reset_index()
    df_eating['week_no'] = pd.to_datetime(df_eating['date']).dt.isocalendar().week
    weeks = pd.DataFrame(df_eating['week_no'].unique())
    weeks.columns = ['week_no']
    symptoms_days_per_week = weeks.merge(symptoms, how='left', left_on='week_no', right_on='week_no')
    avg_symptom_days_per_week = round(symptoms_days_per_week['date'].mean(),1)
    #comparing breakfast, lunch and dinner (e.g. breakfast was documented only 55% of the time, but lunch 87% ...)
    df = df_eating[['date', 'daytime']].groupby('date')[['daytime']].sum().reset_index()
    def detect_breakfast(df_aggregate):
        if 'BREAKFAST' in df_aggregate['daytime']:
            return 1
        else:
            return 0
    def detect_lunch(df_aggregate):
        if 'LUNCH' in df_aggregate['daytime']:
            return 1
        else:
            return 0
    def detect_dinner(df_aggregate):
        if 'DINNER' in df_aggregate['daytime']:
            return 1
        else:
            return 0
    df['breakfast'] = df.apply(detect_breakfast, axis=1)
    df['lunch'] = df.apply(detect_lunch, axis=1)
    df['dinner'] = df.apply(detect_dinner, axis=1)
    breakfast_perc=round(df['breakfast'].sum()*100/df['breakfast'].count(),1)
    lunch_perc=round(df['lunch'].sum()*100/df['breakfast'].count(),1)
    dinner_perc=round(df['dinner'].sum()*100/df['breakfast'].count(),1)
    # generating dataframe for table
    data = {'Überschrift': ['Dokumentierte Tage','Dokumentierte Symptome (Anzahl)','Tage mit Symptomen (Anzahl)','Tage mit Symptomen (Anteil)','Tage mit Symptomen pro Woche (⌀)','Häufigkeit Frühstück','Häufigkeit Mittagessen', 'Häufigkeit Abendessen'],
            'Werte': [usage_days, symptom_count, symptom_days,str(symptom_days_perc) + '%', avg_symptom_days_per_week, str(breakfast_perc ) + '%', str(lunch_perc) + '%', str(dinner_perc) + '%']}
    return pd.DataFrame(data)







        


