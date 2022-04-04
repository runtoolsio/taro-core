"""
BASE 16 COLOURS:
    # Low intensity, dark.  (One or two components 0x80, the other 0x00.)
        - ansiblack
        - ansired
        - ansigreen
        - ansiyellow
        - ansiblue
        - ansimagenta
        - ansicyan
        - ansigray

    # High intensity, bright.
        - ansibrightblack
        - ansibrightred
        - ansibrightgreen
        - ansibrightyellow
        - ansibrightblue
        - ansibrightmagenta
        - ansibrightcyan
        - ansiwhite
"""


class Theme:
    job = 'bold'
    instance = 'ansibrightblack'
    warning = 'ansired'
    state_before_execution = 'ansigreen'
    state_executing = 'ansiblue'
    state_not_executed = 'ansiyellow'
    state_incomplete = 'ansibrightyellow'
    state_failure = 'ansibrightred'
