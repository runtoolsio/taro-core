from tarotools.taro.output import InMemoryOutput


def test_add_and_fetch_all():
    output = InMemoryOutput()
    output.add('source1', 'output1', False)
    output.add('source2', 'output2', True)

    all_output = output.fetch()
    assert all_output == [('output1', False), ('output2', True)]


def test_fetch_specific_source():
    output = InMemoryOutput()
    output.add('source1', 'output1', False)
    output.add('source1', 'output1_2', False)
    output.add('source2', 'output2', True)

    source1_output = output.fetch('source1')
    assert source1_output == [('output1', False), ('output1_2', False)]


def test_fetch_nonexistent_source():
    output = InMemoryOutput()
    output.add('source1', 'output1', False)

    nonexistent_output = output.fetch('source3')
    assert nonexistent_output == []


def test_empty_output():
    output = InMemoryOutput()
    assert output.fetch() == []
    assert output.fetch('source1') == []
