

"""
Custom-made plotting functions for the BesserEsser dashboard

"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from pandas import DataFrame
from collections import Counter
from math import log
from computations import compute_combination_occurrence, get_dates_range, compute_n_rows_n_cols
from constants import DEBUG, ERR_PREFIX, MEALS_MAPPING, TEXT_UNIT_3, A, B, C, D, E
from constants import WEBPAGE_BACKGROUND_COLOR, GRAPH_MARGINS_COLOR, GRAPH_PLOTTING_AREA_COLOR
from constants import DISPLAYNAME  # regex'ed 'displayname' or the original column


# Define the style and colors (optional)
px.defaults.color_discrete_sequence = px.colors.qualitative.T10  #https://plotly.com/python/discrete-color/
px.defaults.color_continuous_scale = px.colors.sequential.Jet    #https://plotly.com/python/builtin-colorscales/



def make_figure(unit, *args, **kwargs):
    """
    TODO
    dispatcher function
    Arguments:
        unit: int, str or unit object (i.e. dash.html.Div with id='unit_3' for example)
    """
    
    # Get int from the client's input 'unit'
    unit_number = int(str.join('', (c for c in str(unit.id if hasattr(unit, 'id') else unit) if c.isdecimal())))

    mapping = {3: make_figure_3,
               4: make_figure_4,
               5: make_figure_5,
               6: make_figure_6,
               7: make_figure_7}
    
    func = mapping.get(unit_number, make_figure_test)
    return func(*args, **kwargs)

    



def get_figure_title(*args):
    """
    Helper function to get an appropriate title for a figure.
    In debugging mode the component_values is the title.
    (In the non-debuging mode 'title' is empty at the moemnt)
    Change the logic here as necessary.
    """
    # Maybe use a list of all titles here? (or import them the the constants module)
    titles = ["Grafik 1", ...]   # e.g. titles[args] id type(args) is int

    # hackish no time
    for _ in range(5):
        if hasattr(args, '__len__') and len(args)==1:
            args = args[0]
        else:
            break
    
    if args is None:
        return ""

    if hasattr(args, '__len__') and len(args)>1 and type(args[0]) is DataFrame:
        df, args = args[0], args[1:]

    # haskich, provisional
    args = args[3:]  # drop off the Store obejct jsons

    title = str(args) if DEBUG == True else args.get('title') if type(args) is dict else None 
    return title
    


def _get_colors(items, color=None):
    """
    helper function 
    only for make_tiles_plot and make_pie_plot
    """
    mapping = {A: px.colors.sequential.Blues,   # all days
             B: px.colors.sequential.Greens,    # days with no symptoms
             C: px.colors.sequential.Reds,      # days with symptoms
             D: px.colors.sequential.Oranges,   # days prior to symptoms
             "reds": px.colors.sequential.Reds, 
             "red": px.colors.sequential.Reds,
             None: px.colors.sequential.Turbo}  # just in case
    
    colors = mapping.get(color, px.colors.sequential.Turbo)[1:]    # drop of the white 
    n = len(items)
    return [colors[i * len(colors) // n] for i in range(n)][::-1]  # reverse - from dark to light



def make_tiles_plot(items=None, values=None, color=None):
    """
    Makes a simple Treemap plot (non-hierarchical aka tiles-plot)
    Returns a plotly figure.
    """

    fig = px.treemap(names=items,
                     values=values, 
                     parents=[""]*len(items),    
                     color_discrete_sequence=_get_colors(items, color),                      
                     )  
    fig.update_layout(hovermode=None)   # how to deactivate tooltips?
    fig.update_coloraxes(showscale=False) 
    return fig



def make_pie_plot(items, color):
    """
    Legacy function.
    Makes a simple pie chart with equal slices.
    (plotly-go is used because it was not possibly to deactivate the unnecessary 
    hoverover-tooltips in the figure returned by plotly.express): hoverinfo='skip'
    TODO: test and debug this function to make it work for all cases (df / array)
    """

    # line between slices
    LINE_WIDTH = 2

    # To avoid px error if "unhashable type"
    items = tuple(items)

    fig = go.Figure(data=go.Pie({'labels': items}, hoverinfo='skip'))
    fig.update_traces(marker=dict(colors=_get_colors(items, color)))
    fig.update_traces(textposition='inside', textinfo='label')
    fig.update_layout(hovermode=None, showlegend=False)
    fig.update_traces(marker=dict(line=dict(color='#000000', width=LINE_WIDTH)))
    return fig


def make_pie(items, **kwargs):
    """
    A helper function for make_pies_plot.
    Makes a simple pie chart with equal slices.
    (plotly-go is used because it was not possibly to deactivate the unnecessary 
    hoverover-tooltips in the figure returned by plotly.express): hoverinfo='skip'
    """
    return go.Pie({'labels': items}, hoverinfo='skip', **kwargs)


def make_pies_plot(list_of_lists, color):
    """
    list_of_lists: [[milk, sugar], [apple, milk, pizza]]
    """

    # line between slices
    LINE_WIDTH = 2

    # just in case
    if not list_of_lists:
        return no_data_available()
    
    n_rows, n_cols = compute_n_rows_n_cols(len(list_of_lists))
    specs = [[{'type':'domain'}]*n_cols]*n_rows
    fig = make_subplots(n_rows, n_cols, 
                        specs=specs, 
                        #horizontal_spacing = 0.01, 
                        # vertical_spacing=0.05
                        )

    for i,items in enumerate(list_of_lists):
        row, col = (e+1 for e in divmod(i, n_cols))  #+1 because one-based
        fig.add_trace(make_pie(items, name=str(i)), row=row, col=col)
        fig.update_traces(marker=dict(colors=_get_colors(items, color)),
                          selector=({'name': str(i)}))

    fig.layout.template = None # to slim down the output
    #fig.update_traces(marker=dict(colors=_get_colors(items, color)))
    fig.update_traces(textposition='inside', textinfo='label')
    fig.update_layout(hovermode=None, showlegend=False)
    fig.update_traces(marker=dict(line=dict(color='#000000', width=LINE_WIDTH)))
    return fig



def make_figure_3(df_eating, df_symptoms, debugging_info=None):
    """
    Ernährungs- und Symptomtagebuch

    Plotly figure object (i.e. plot) for Unit 1.

    Args:
        todo
    Returns:
        plotly figure object
    """

    # Define colors for BREAKFAST, LUNCH, DINNER
    COLORS = ["yellow", "green", "blue"]

    # unite the dates from the two df's
    dates = sorted(set(df_eating['date']).union(df_symptoms['date']))
    n_dates = len(dates)

    # no time to test this for bugs - (no data -> no plot)
    if n_dates == 0:
        return no_data_available()

    # make a single timeline with dates (min/max from theunion of the two df's)
    date_range = get_dates_range(df_eating, df_symptoms, False)
    n_dates = len(date_range)

    # no time to guarantee this for future
    x = date_range
    y = [3 if date in df_symptoms['date'].values else 0 for date in date_range]
  
    # Set the size for the circle marker dynamically (maybe play with logarithm?)
    arbitrary_number = 250                  # adjust manually
    non_linear_scaler = 0.5 * log(n_dates)  # play with this
    marker_size = int(arbitrary_number / n_dates * non_linear_scaler)  # no time to adjust this

    # Bars = symptom
    traces = [go.Bar(x=x, y=y, 
                     name=TEXT_UNIT_3,  # "Tage mit Symptomen"
                     hoverinfo='x',
                     marker=dict(color='red', opacity=0.6)),]  #line=dict(width=marker_size)
    
    # plot the groups of dots: for BREAKFAST, LUNCH, DINNER
    for i, daytime, mahlzeit, color in zip(range(3, 0, -1), 
                                            *zip(*list(MEALS_MAPPING.items())), 
                                            COLORS):
        df_temp = df_eating.loc[df_eating['daytime'] == daytime, ['date',  DISPLAYNAME]].groupby('date').agg(list).sort_index()
        df_temp[DISPLAYNAME] = df_temp[DISPLAYNAME].apply(", ".join)
        df_temp['bar_height'] = i - 0.5   
        trace = go.Scatter(x=df_temp.index, y=df_temp['bar_height'], 
                           mode='markers', name=mahlzeit,
                           text=df_temp[DISPLAYNAME], hoverinfo='text',  #'x+y+text'
                           marker=go.scatter.Marker(color=color, 
                                                    opacity=0.6, size=marker_size ))   #size=marker_size  
        traces.append(trace)
    
    fig = go.Figure(data=traces)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)
    fig.update_layout(yaxis = dict(tickmode = 'array', 
                                   tickvals = [2.5, 1.5, 0.5], 
                                   ticktext = list(MEALS_MAPPING.values())))
    fig.update_layout(height=300)  # adjust manually
    fig.update_layout(title=get_figure_title(debugging_info))
    return fig



def make_figure_4(df, color=None, debugging_info=None):
    """
    Welche Lebensmittel sind am meisten konsumiert
    """
    TOP_N = 10

    # no data -> no plot
    if df is None or len(df)==0:
        return no_data_available()
    
    df = df[DISPLAYNAME].value_counts().head(TOP_N).to_frame().reset_index()

    fig = make_tiles_plot(items=df[DISPLAYNAME], 
                          values=df['count'],  # generic name by pandas value_counts()
                          color=color)
    fig.update_layout(title=get_figure_title(debugging_info))
    return fig



def make_figure_5(df, color=None, debugging_info=None):
    """
    Welche Lebensmittel wurden unmittelbar vor der Symptomentstehung gegessen
    """
    TOP_N = 10

    # no data -> no plot
    if df is None or len(df)==0:
        return no_data_available()

    sr = df[DISPLAYNAME].value_counts().head(TOP_N)

    # just in case
    if sr is None or len(sr)==0:
        return no_data_available()

    fig = make_tiles_plot(items=sr.index, values=sr.values, color=color)
    fig.update_layout(title=get_figure_title(debugging_info))
    return fig


def make_figure_6(df, color=None, debugging_info=None):
    """
    Wie sieht ein typisches Frühstück, Mittagessen oder Abendessen aus
    TODO: tidy this function up
    """

    TOP_N = 3   # can choose any TOP_N from 1

    # no data -> no plot
    if df is None or len(df)==0:
        return no_data_available()

    arr = df[['date', 'daytime', DISPLAYNAME]].groupby(['date','daytime']).agg(list).values.ravel()
    sets = [frozenset(e) for e in arr]
    list_of_lists = [sorted(array)      # array = a set representing a meal
                     for array,count in 
                     sorted(Counter(sets).most_common(TOP_N), 
                            key=lambda t: (t[1], -len(t[0])), # -len() puts shorter sets (of the same rank)
                            reverse=True)                     # first because a shorter one can be a subset
                     if count>1]                              # of a longer one by chance
    
    # just in case
    if not list_of_lists:  # i.e. if len(list_of_lists)==0 (but safer, cos no time to test for edge cases)
        return no_data_available()
    
    # make a plotly-figure with multiple "pie-charts"
    fig = make_pies_plot(list_of_lists, color)
    fig.update_layout(title=get_figure_title(debugging_info))
    return fig



def make_figure_7(df, n_components, color=None, debugging_info=None):
    """
    Welche Lebensmittel werden (in einer bestimmten Mahlzeit) kombiniert
    """

    TOP_N = 5 # number of tiles on the treemap-plot

    # no data -> no plot
    if df is None or len(df)==0:
        return no_data_available()

    df_aggregated = df[['date', 'daytime', DISPLAYNAME]].groupby(['date','daytime']).agg(tuple)
    meals = [frozenset(tuple(e[0])) for e in df_aggregated.values.tolist()]
    
    counter = compute_combination_occurrence(meals, cardinality=n_components)
    df_for_plot = DataFrame([(", ".join(e[0]), e[1]) for e in counter.most_common(TOP_N)], columns=["combination", "count"])
    
    # if no data - just in case
    if df_for_plot is None or len(df_for_plot)==0:
        return no_data_available()
    
    fig = make_tiles_plot(items=df_for_plot['combination'], 
                          values=df_for_plot['count'],
                          color=color)
    fig.update_layout(title=get_figure_title(debugging_info))
    return fig



def make_figure_test(df, *args):
    """Generic plotly plot for testing"""
    fig = px.bar(y=[1,2,3],
                 color_discrete_sequence=['grey'])
    fig.update_layout(title=get_figure_title(*args))
    return fig



def no_data_available():
    TEXT = "keine Daten vorhanden"
    fig = go.Figure(data=go.Scatter(y=[0,], mode='lines'))
    fig.layout.title = go.layout.Title(text=TEXT, font_size=30)
    fig.update_layout(paper_bgcolor=GRAPH_MARGINS_COLOR,
                      plot_bgcolor=GRAPH_PLOTTING_AREA_COLOR,
                      title=dict(x=0.5, y=0.5, font={'size':30, 'color': 'grey'}))
    fig.update_layout(xaxis_title="", yaxis_title="")
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)
    fig.update_layout(xaxis = dict(tickmode = 'array', tickvals = [], ticktext = []))
    fig.update_layout(yaxis = dict(tickmode = 'array', tickvals = [], ticktext = []))
    return fig








