
from collections import defaultdict as dd
from functools import partial
from typing import *

import numpy as np
import pandas as pd
from pandas.api import types as pd_types

from guesttracker import config as cf
from guesttracker import dt, getlog
from guesttracker.utils import xterm_color as xc

if TYPE_CHECKING:
    from matplotlib.colors import LinearSegmentedColormap
    from pandas.io.formats.style import Styler

if not cf.AZURE_WEB:
    from matplotlib.colors import TwoSlopeNorm, rgb2hex
    from seaborn import diverging_palette
    cmap_default = diverging_palette(240, 10, sep=10, n=21, as_cmap=True)  # default cmap red blue


log = getlog(__name__)


def left_justified(df, header=False):
    formatters = {}
    for li in list(df.columns):
        max_ = df[li].str.len().max()
        form = '{{:<{}s}}'.format(max_)
        formatters[li] = partial(str.format, form)

    return df.to_string(formatters=formatters, index=False, header=header)


def format_dtype(df, formats):
    """Match formats to df.dtypes
    - format can be either string or func
    - formats = {'int64': '{:,}'}
    - df.dtypes = {'Unit': dtype('O'),
                datetime.dt(2020, 3, 1): dtype('int64'),"""
    m = {}
    for key, fmt in formats.items():
        # need str(val) to convert np dtype to string to avoid failed comparisons to pd.Int64 etc
        m.update({col: fmt for col, val in df.dtypes.to_dict().items() if str(val) == key})

    return m


def apply_formats(style: 'Styler', formats: dict) -> 'Styler':
    # apply other formats that may not be defaults
    m = format_dtype(df=style.data, formats=formats)
    return style.format(m)


def default_number_format(x: float) -> str:
    # give default number format to vals in the 'numeric col mask', handle nulls
    if not pd.isnull(x):
        return f'{x:,.0f}' if x > 1e3 else f'{x:,.2f}'
    else:
        return ''


def format_date(x: dt) -> str:
    return f'{x:%Y-%m-%d}' if not pd.isnull(x) else ''


def format_datetime(x: dt) -> str:
    return f'{x:%Y-%m-%d  %H:%M}' if not pd.isnull(x) else ''


def alternating_rows(style: 'Styler') -> 'Styler':
    s = []
    s.append(dict(
        selector='tbody tr:nth-child(even)',
        props=[('background-color', '#E4E4E4')]))

    return style.pipe(add_table_style, s)


def alternating_rows_outlook(style: 'Styler', outlook: bool = True) -> 'Styler':
    """Highlight odd rows background color grey
    - row slice is list of index labels"""

    # NOTE both of these work!! just need to pass the SLICE of INDEX as first arg, not df itself
    # subset = pd.IndexSlice[style.data.iloc[1::2].index, :]
    subset = pd.IndexSlice[style.data.index[1::2], :]

    if outlook:
        style = style.apply(
            lambda df: pd.DataFrame(data='background-color: #E4E4E4;', index=df.index, columns=df.columns),
            subset=subset,
            axis=None)

    return style


def add_table_style(style: 'Styler', s: List[Any], do: bool = True) -> 'Styler':
    if not do:
        return style

    if not style.table_styles is None:
        style.table_styles.extend(s)
    else:
        style.set_table_styles(s)

    return style


def add_table_attributes(style, s, do=True):
    # NOTE this may not work with multiple of same attrs eg style=red, style=10px
    if not do:
        return style

    attrs = style.table_attributes
    if not attrs is None:
        s = f'{attrs} {s}'

    style.set_table_attributes(s)
    return style


def string_to_attrs(s):
    # convert string of table attrs to dict
    # split on '=' and make dict of {odds: evens}
    lst = s.split('=')
    return dict(zip(lst[::2], lst[1::2]))


def set_col_alignment(style, col_name, alignment):
    i = style.data.columns.get_loc(col_name)
    s = [dict(
        selector=f'td:nth-child({i + 1})',  # css table 1-indexed not 0
        props=[('text-align', alignment)])]

    return style \
        .pipe(add_table_style, s)


def set_column_style(mask, props):
    # loop columns in mask, get index, set column style
    s = []
    for i, v in enumerate(mask):
        if v is True:
            s.append(dict(
                selector=f'td:nth-child({i + 1})',  # css table 1-indexed not 0
                props=[props]))

    return s


def set_column_widths(style, vals, hidden_index=True, outlook=False):
    # vals is dict of col_name: width > {'Column Name': 200}
    s = []
    offset = 1 if hidden_index else 0

    if not outlook:
        for col_name, width in vals.items():

            # some tables have different cols for monthly/weekly (F300 SMR)
            if col_name in style.data.columns:
                icol = style.data.columns.get_loc(col_name) + offset
                s.append(dict(
                    selector=f'th.col_heading:nth-child({icol})',
                    props=[('width', f'{width}px')]))

        return style.pipe(add_table_style, s)

    else:
        # outlook - need to apply width to each cell individually
        return style.apply(col_width_outlook, axis=None, vals=vals)


def default_style(df: pd.DataFrame, outlook: bool = False) -> 'Styler':
    """Dataframe general column alignment/number formatting"""

    # allow passing in styler or df
    from pandas.io.formats.style import Styler
    if isinstance(df, Styler):
        df = df.data

    cols = [k for k, v in df.dtypes.items() if v == 'object']  # only convert for object cols
    df[cols] = df[cols].replace('\n', '<br>', regex=True)

    font_family = 'Tahoma, Geneva, sans-serif;' if not outlook else 'Calibri'

    s = []
    m = cf.config['color']

    # thead selects entire header row, instead of individual header cells. Not sure if works for outlook
    s.append(dict(
        selector='thead',
        props=f'text-align: center; background: {m["thead"]}'))
    s.append(dict(
        selector='th, td',
        props=f'font-family: {font_family}; padding: 2.5px 5px;'))
    s.append(dict(
        selector='table',
        props='border: 1px solid #000000; margin-top: 0px; margin-bottom: 2px'))

    def is_np(item):
        return issubclass(type(item), np.dtype)

    # numeric_mask = df.dtypes.apply(lambda x: is_np(x) and issubclass(np.dtype(str(x).lower()).type, np.number))
    numeric_mask = df.dtypes.apply(
        lambda x: pd_types.is_numeric_dtype(x) and not pd_types.is_bool_dtype(x))
    bool_mask = df.dtypes.apply(lambda x: pd_types.is_bool_dtype(x))
    date_mask = df.dtypes.apply(lambda x: is_np(x) and issubclass(np.dtype(str(x).lower()).type, np.datetime64))

    s.extend(set_column_style(mask=numeric_mask, props=('text-align', 'right')))
    s.extend(set_column_style(mask=~numeric_mask, props=('text-align', 'left')))
    s.extend(set_column_style(mask=bool_mask, props=('text-align', 'center')))
    s.extend(set_column_style(mask=date_mask, props=('text-align', 'center')))

    border = ' border: 1px solid #000000;' if outlook else ''
    table_attrs = f'style="border-collapse: collapse;{border}"'

    # NOTE kinda messy/duplicated here
    formats = {'Int64': '{:,}', 'int64': '{:,}', 'datetime64[ns]': '{:%Y-%m-%d}'}
    m_fmt = format_dtype(df=df, formats=formats)

    return df.style \
        .format(na_rep='') \
        .format(default_number_format, subset=pd.IndexSlice[:, df.columns[numeric_mask]], na_rep='') \
        .pipe(format_dict, m_fmt) \
        .pipe(add_table_style, s) \
        .pipe(alternating_rows_outlook, outlook=outlook) \
        .pipe(add_table_attributes, s=table_attrs)


def format_dict(style: 'Styler', fmt: dict) -> 'Styler':
    """Convenience func to pipe formatting with dict, and apply only to subset

    Parameters
    ----------
    style : Styler
    fmt : dict
        eg {'col_1': '{:.2f}'}

    Returns
    -------
    Styler
    """
    if len(fmt) == 0:
        return style

    cols = style.data.columns.tolist()  # + [style.data.index.name]
    fmt = {c: v for c, v in fmt.items() if c in cols}

    return style.format(fmt, subset=list(fmt.keys()), na_rep='')


def df_empty(df: pd.DataFrame, theme: str = 'light') -> pd.DataFrame:
    """Return df with same shape as original
    - filled with 'background-color: inherit'

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    fill_val = 'background-color: inherit' if theme == 'light' else np.NAN
    return pd.DataFrame(
        data=fill_val,
        index=df.index,
        columns=df.columns)


def hide_headers(style):
    # use css selector to hide table headers
    s = []
    s = [dict(selector='.col_heading',
              props=[('display', 'none')])]
    return style.pipe(add_table_style, s)


def col_width_outlook(df, vals):
    df1 = df_empty(df)
    for col, width in vals.items():
        df1[col] = f'width: {width};'

    return df1


def get_defaults(theme: str) -> Tuple[str, str]:
    """Get background default colors
    - light = reporting + emailing
    - dark = GUI

    Parameters
    ----------
    theme : str
        light | dark

    Returns
    -------
    Tuple[str, str]
    """
    return dict(light=('inherit', 'black'), dark=('', '')).get(str(theme).strip())


def format_cell(bg, t='black'):
    return f'background-color: {bg};color: {t};'


def highlight_greater(df, ma_target):
    # Highlight cells good or bad where MA > MA Target
    # pass ma_target series separately to not apply any styles
    m = cf.config['color']
    bg, t = m['bg'], m['text']

    m = df['MA'] > ma_target

    df1 = pd.DataFrame(index=df.index, columns=df.columns)
    result = np.where(m, format_cell(bg['good'], t['good']), format_cell(bg['bad'], t['bad']))
    for col in df1.columns:
        df1[col] = result

    # for col in ('Unit', 'Target Hrs Variance'):
    #     if col in df.columns:
    #         df1[col] = df1['MA']
    return df1


def highlight_yn(df, color_good='good', theme='light'):
    m = cf.config['color']
    bg, t = m['bg'], m['text']
    default_bg, default_t = get_defaults(theme)

    m1, m2, m3 = df == 'Y', df == 'N', df == 'S'  # create three boolean masks

    where = np.where
    data = where(
        m1,
        format_cell(bg[color_good], t[color_good]),
        where(
            m2,
            format_cell(bg['bad'], t['bad']),
            where(
                m3,
                format_cell(bg['lightyellow'], 'black'),
                f'background-color: {default_bg}')))

    return pd.DataFrame(data=data, index=df.index, columns=df.columns)


def highlight_multiple_vals(
        df: pd.DataFrame,
        m: dict,
        convert: bool = False,
        theme: str = 'light',
        none_inherit: bool = True) -> pd.DataFrame:
    """Highlight multiple vals in df based on input from style.apply

    Parameters
    ----------
    m : dict
        {val: (bg_color, t_color)}
    convert : bool
        if true, convert color map to bg/text first eg 'lightorange' to hex color #fffff
    theme : str
        used when converting color code
    none_inherit : bool
        some tables just want '' instead of 'inherit' for blanks (eg OilSamples table in EL GUI)
    """
    if convert:
        m = convert_color_code(m_color=m, theme=theme)

    val = 'inherit' if none_inherit else ''
    m |= {None: (val, val)}

    m_replace = {k: format_cell(bg=v[0], t=v[1]) for k, v in m.items()}

    # fill everything that won't be filled with color as None to be 'inherit'
    return df \
        .astype('object') \
        .where(df.isin(m.keys()), other=None) \
        .replace(m_replace)


def highlight_flags(df, m, suffix='_fg', theme='light', **kw):
    """Highlight flagged columns for oil samples"""
    df1 = highlight_multiple_vals(df=df, m=m, theme=theme, **kw)

    flagged_cols = [col for col in df.columns if suffix in col]

    for col in flagged_cols:
        col2 = col.replace(suffix, '')
        df1[col2] = df1[col]
        df1[col] = ''

    return df1


def highlight_expiry_dates(s, theme='light'):
    """Highlight FC Dates approaching expiry

    Parameters
    ---------
    s : pd.Series
        Only fmt single column at a time for now
    theme : str
        Dark or light theme for app or reports
    """
    m = cf.config['color']
    bg, t = m['bg'], m['text']

    s1 = pd.Series(index=s.index, dtype='object')  # blank series
    s_days_exp = (dt.now() - s).dt.days  # days as int btwn now and date in column

    # filter column where date falls btwn range
    s1[s_days_exp.between(-90, -30)] = format_cell(bg['lightyellow'], 'black')
    s1[s_days_exp.between(-30, 0)] = format_cell(bg['lightorange'], 'black')
    s1[s_days_exp > 0] = format_cell(bg['lightred'], 'white')
    s1[s1.isnull()] = format_cell(*get_defaults(theme))  # default for everything else

    return s1


def highlight_numeric(style: 'Styler', target_col: str, opr: Callable, val: float, **kw) -> pd.DataFrame:

    # mask where col less/greater than etc
    df = style.data
    mask = opr(df[target_col], val)

    return style.apply(
        highlight_val,
        subset=target_col,
        axis=None,
        masks=mask,
        target_col=target_col,
        **kw)


def highlight_val(
        df: pd.DataFrame,
        target_col: str,
        val: str = None,
        bg_color: str = None,
        t_color: str = None,
        other_cols: List[str] = None,
        theme: str = 'light',
        masks: Union[pd.Series, Dict[str, pd.Series]] = None) -> pd.DataFrame:
    """Highlight single or multiple string vals based on single val: color, or complex dict masks

    Parameters
    ----------
    df : pd.DataFrame
    target_col : str
        col to apply func to
    val : str, optional
        val to replace, by default None
    bg_color : str, optional
        bg color to use (if not given in masks), by default None
    t_color : str, optional
        text color, by default None
    other_cols : List[str], optional
        mirror highlighting in target_col to other_cols, by default None
    theme : str, optional
        background theme, by default 'light'
    masks : Union[pd.Series, Dict[str, pd.Series]], optional
        single boolean mask, or dict of {color: mask}, by default None

    Returns
    -------
    pd.DataFrame
    """
    m = cf.config['color']
    bg, t = m['bg'], m['text']
    default_bg, default_t = get_defaults(theme)
    df1 = df_empty(df, theme=theme)
    t_color = t_color or 'black'

    if not isinstance(masks, dict):
        if bg_color is None:
            raise RuntimeError(f'must provide bg_color [{bg_color}]')

        if masks is None:
            masks = df[target_col].astype(str).str.lower() == val.lower()

        masks = {bg_color: masks}

    # allow passing in premade mask if not str columns
    for bg_color, mask in masks.items():
        df1.loc[mask, target_col] = format_cell(bg[bg_color], t[t_color])

    df1[target_col] = df1[target_col].fillna(f'background-color: {default_bg}')

    if other_cols:
        for col in other_cols:
            df1[col] = df1[target_col]

    return df1


def pipe_highlight_alternating(style, color, theme, subset=None):
    return style.apply(highlight_alternating, subset=subset, color=color, theme=theme)


def highlight_alternating(
        s: Union[pd.DataFrame, pd.Series],
        color: str = 'navyblue',
        theme: str = 'light',
        is_hex: bool = True,
        anchor_col: str = None):
    """Loop df column and switch active when value changes. Kinda ugly but works."""
    colors = cf.config['color']
    default_bg, default_t = get_defaults(theme)

    if is_hex:
        color = colors['bg'][color]

    active = 1
    prev = ''

    if isinstance(s, pd.Series):
        is_series = True
        df = None
    else:
        is_series = False
        df = s.copy()
        s = df[anchor_col]  # get anchor series from df

    s1 = pd.Series(index=s.index, dtype='object')  # NOTE could make this s_empty()

    # iterrows iterates tuple of (index, vals)
    for row in s.iteritems():
        idx = row[0]
        val = row[1]  # [0]
        if not pd.isnull(val):
            if not val == prev:
                active *= -1

            prev = val

        if active == 1:
            css = format_cell(bg=color, t='white')
        else:
            css = format_cell(bg=default_bg, t=default_t)

        s1.loc[idx] = css

    if not is_series:
        # duplicate series to entire dataframe
        n = df.shape[1]
        data = np.tile(s1.values, (n, 1)).T
        s1 = pd.DataFrame(data, index=df.index, columns=df.columns)

    return s1


def highlight_totals_row(style, exclude_cols=(), n_cols=1, do=True):
    # highlight the last row of given dataframe
    if not do:
        return style
    bg = cf.config['color']['thead']
    subset = pd.IndexSlice[style.data.index[-1 * n_cols:], :]

    return style.apply(
        lambda x: [format_cell(bg, 'white') if not x.index[i]
                   in exclude_cols else 'background-color: inherit' for i, v in enumerate(x)],
        subset=subset,
        axis=1)


def highlight_accepted_loads(df):
    m = cf.config['color']
    bg, t = m['bg'], m['text']

    # >120% cant have any loads
    m_cols = {'110 - 120%': 0.1, '>120%': 0.0}

    m = [df[col].iloc[0] <= threshold for col, threshold in m_cols.items()]

    data = np.where(
        m,
        format_cell(bg['goodgreen'], t['goodgreen']),
        format_cell(bg['bad'], t['bad'])).reshape((1, 2))

    return pd.DataFrame(data=data, index=df.index, columns=df.columns)


def bold_columns(df):
    return pd.DataFrame(data='font-weight: bold;', index=df.index, columns=df.columns)


def set_borders(style):
    s = [dict(
        selector='th, td',
        props=[
            ('border', '1px solid black'),
            ('padding', '3px, 5px')])]
    return style.pipe(add_table_style, s=s)


def write_html(html, name=None):
    if name is None:
        name = 'temp'

    p = cf.p_topfolder.parent / f'{name}.html'
    with open(str(p), 'w+') as file:
        file.write(html)


def convert_color_code(m_color: dict, theme: str = 'light') -> Dict[str, Tuple[str]]:
    """Convert color names to bg/text color codes from config
    - used to pass in to highlight_multiple_vals

    Parameters
    ---------
    m_color : dict
        dict of named color vals eg {'S1 Service': 'lightyellow'}
    """
    m = cf.config['color']
    bg, t = m['bg'], m['text']
    default_bg, default_t = get_defaults(theme)

    return {k: (bg.get(v, default_bg), t.get(v, default_t)) for k, v in m_color.items()}


def background_grad_center(
        s: pd.Series,
        cmap: 'LinearSegmentedColormap' = None,
        center: float = 0,
        vmin: float = None,
        vmax: float = None) -> Union[List[str], pd.DataFrame]:
    """Style column with diverging color palette centered at value, including dark/light text formatting
    - modified from https://github.com/pandas-dev/pandas\
    /blob/b7cb3dc25a5439995d2915171c7d5172836c634e/pandas/io/formats/style.py

    Parameters
    ----------
    s : pd.Series
        Column to style
    cmap : matplotlib.colors.LinearSegmentedColormap, optional
        default self.cmap
    center : int, optional
        value to center diverging color, default 0
    vmin : float, optional
        min value, by default None
    vmax : float, optional
        max value, by default None

    Returns
    -------
    list
        list of background colors for styler
    """

    vmin = vmin or s.values.min()  # type: float
    vmax = vmax or s.values.max()  # type: float

    # vmin/vmax have to be outside center
    if vmin >= center:
        vmin = center - 1

    if vmax <= center:
        vmax = center + 1

    norm = TwoSlopeNorm(vmin=vmin, vcenter=center, vmax=vmax)

    text_color_threshold = 0.408  # default from pandas

    def relative_luminance(rgba) -> float:
        """Check if rgba color is greater than darkness threshold"""
        r, g, b = (
            x / 12.92 if x <= 0.04045 else ((x + 0.055) / 1.055) ** 2.4
            for x in rgba[:3])

        return 0.2126 * r + 0.7152 * g + 0.0722 * b

    def css(rgba) -> str:
        dark = relative_luminance(rgba) < text_color_threshold
        text_color = '#f1f1f1' if dark else '#000000'
        return format_cell(bg=rgb2hex(rgba), t=text_color)

    if cmap is None:
        cmap = cmap_default

    if not cmap is None:
        rgbas = cmap(norm(s.astype(float).values))

    if s.ndim == 1:
        return [css(rgba) for rgba in rgbas]
    else:
        return pd.DataFrame(
            [[css(rgba) for rgba in row] for row in rgbas],
            index=s.index,
            columns=s.columns)


def convert_stylemap_index(style: 'Styler') -> Dict[Tuple[int, int], List[Tuple[str, str]]]:
    """Convert irow, icol stylemap to df named index
    - used for df_terminal_color
    - NOTE styler saves everything, so if multiple styles are applied, this will only use the first

    Returns
    -------
    Dict[Tuple[int, int], List[Tuple[str, str]]]
        {(0, 4): [('background-color, '#fef0f0'), ('color', '#000000')]}
    """
    style._compute()
    df = style.data

    return {(df.index[k[0]], df.columns[k[1]]): v for k, v in style.ctx.items()}


def style_ctx_to_dict(style: 'Styler', named_keys: bool = False) -> Dict[Tuple[int, int], Dict[str, str]]:
    """Convert Styler ctx dict to dict with str keys instead of tuples
    - NOTE not used currently
    - NOTE only returns LAST set background-color or color
    - eg
        - {(0, 3): [('background-color', 'inherit'), ('color', 'black')],
        - {(0, 3): {'background-color': 'inherit', 'color': 'black'},

    Parameters
    ----------
    style : Styler

    Returns
    -------
    Dict[Tuple[int, int], Dict[str, str]]
    """
    style._compute()

    if not named_keys:
        return {k: {s[0]: s[1] for s in v} for k, v in style.ctx.items()}
    else:
        df = style.data
        return {(df.index[k[0]], df.columns[k[1]]): {s[0]: s[1] for s in v}
                for k, v in style.ctx.items()}


def convert_stylemap_index_color(
        style: 'Styler',
        as_qt: bool = True,
        first_val: bool = True) -> Tuple[dict, dict]:
    """Convert (irow, icol) stylemap to dict of {col: {row: QColor(value)}}

    eg stylemap = {(0, 4): ['background-color: #fef0f0', 'color: #000000']}


    Parameters
    ----------
    style: pd.Styler
    as_qt: bool
        return QColor or just hex
    first_val: bool
        style.ctx could have had multiple style_vals set for eg background-color
        allow selecting first or last val

    Returns
    -------
    tuple
        (background, text) of
            {col: {row: QColor}}
    """
    from PyQt6.QtGui import QColor

    m_background = dd(dict)
    m_text = dd(dict)
    df = style.data

    if len(style.ctx) == 0:
        style._compute()

    def set_color(style_vals: List[Tuple[str]], i: int) -> Union[QColor, str]:
        """Convert list of tuples of colors to single color value
        - may only have background-color, not color

        Parameters
        ----------
        style_vals : List[Tuple[str]]
            list of [('background-color', '#E4E4E4'),]
        i : int
            either first or last background-color/color tuple

        Returns
        -------
        Union[QColor, str]
            single color QColor val or str eg '#E4E4E4'
        """
        try:
            # style_vals can be [] sometimes?
            if style_vals:
                color = style_vals[i][1]
                color_out = color if not color == '' else None
                return QColor(color_out) if as_qt and not color_out is None else color_out
        except Exception:
            log.warning(f'Failed to get style from: {style_vals}')
            return None

    first_last = 0 if first_val else -1

    # filter out blank background-color: '' and color: ''
    # NOTE not sure if this extra step slows down GUI table loading a noticeable amount or not
    # NOTE pd.Int64 col with pd.NA = "[-]" instead of ['background-color: ...']
    # t = tuple = ('background-color', '#d7a4df')
    ctx_filtered = {k: [t for t in v if not t[1] == ''] for k, v in style.ctx.items()}

    for (irow, icol), style_vals in ctx_filtered.items():
        row, col = df.index[irow], df.columns[icol]

        # filter style_vals to only include one background-color and one color
        bg_vals = [t for t in style_vals if t[0] == 'background-color']

        text_vals = list(set(style_vals) - set(bg_vals))

        # if bg is 'inherit', remove it?
        # NOTE this might fail if two 'inherit'
        if len(bg_vals) > 1:
            bg_vals = [t for t in bg_vals if not t[1] == 'inherit']

        m_background[col][row] = set_color(bg_vals, i=first_last)
        m_text[col][row] = set_color(text_vals, i=first_last)

    return m_background, m_text


def df_terminal_color(df: pd.DataFrame, style: 'Styler') -> pd.DataFrame:
    """Create string version of df with terminal color codes
    - NOTE only USES first of background-color or color
    - only SETS text color
    """

    # map hex codes to terminal codes
    df = df.copy()

    palette = dict(
        black=30,
        red=31,
        green=32,
        yellow=33,
        blue=34,
        cyan=36,
        white=37,
        royalblue=4,
        # underline=4,
        # reset=0
    )

    reset = '\033[0m'

    # 38;5;{color} = foreground
    # 48;5;{color} = background
    _fg = lambda x: '\033[38;5;{}m'.format(x) if not x == '' else ''

    # create full color codes as a dict comp, only used for named colors
    # palette = {k: f'\033[{color_code}m' for k, color_code in palette.items()}
    palette = {k: f'{_fg(color_code)}' for k, color_code in palette.items()}
    # print(palette)

    m = convert_stylemap_index(style)
    # m = style_ctx_to_dict(style=style, named_keys=True)

    # convert from [('color', 'red')] to 'red'
    # just uses first of background-color or color I think?
    m = {k: v[0][-1] for k, v in m.items()}

    # get unique cols
    cols = list(set([k[1] for k in m.keys()]))

    df[cols] = df[cols].astype(str)

    for (row, col), color in m.items():

        # hex, convert to x256
        if '#' in color:
            color = xc.rgb2short(color)[0]
        elif 'inherit' in color:
            color = ''

        new_color = '{}{}{}'.format(
            palette.get(color, _fg(color)),
            df.loc[row, col],
            reset)

        df.loc[row, col] = new_color

    return df


def extend_hidden_cols(style: 'Styler', subset: list) -> 'Styler':
    new_idxs = [style.data.columns.get_loc(item) for item in subset]
    style.hidden_columns = np.array(list(style.hidden_columns) + new_idxs)
    return style
