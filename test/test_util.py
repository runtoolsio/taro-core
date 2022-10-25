"""
Tests :mod:`util` module
Description: Util module tests
"""
import pytest

from taro import util


def test_truncate_with_suffix():
    assert util.truncate('trunc_this_', len('trunc_this_') - 1) == 'trunc_this'
    assert util.truncate('hey_trunc_this', len('hey_trunc_this') - 1, 'ignore...') == 'hey_ignore...'
    assert util.truncate('trunc_this', len('trunc_this') - 1, 'ignore...') == 'ignore...'
    assert util.truncate('no_trunc', len('no_trunc')) == 'no_trunc'
    assert util.truncate('no_trunc', len('no_trunc'), 'ignore..') == 'no_trunc'


def test_suffix_larger_than_max():
    with pytest.raises(ValueError):
        util.truncate('whatever here', 3, '1234')
