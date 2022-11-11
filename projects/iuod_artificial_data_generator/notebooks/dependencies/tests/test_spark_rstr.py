# Databricks notebook source
# MAGIC %run ../spark_rstr

# COMMAND ----------

# MAGIC %run ../../../../tests/test_helpers

# COMMAND ----------

import re
import unittest
import random


class TestContext:
  def __init__(self, nrows=1):
    self.df = spark.range(nrows)
    
  def assert_matches(self, pattern, column):
    value = self.df.select(column.alias('value')).first()['value']
    errmsg = '{} does not match {}'.format(value, pattern)
    assert re.match(pattern, value), errmsg
    
    
context = TestContext()
assert_matches = context.assert_matches

# COMMAND ----------

STAR_PLUS_LIMIT = 10  # Reduced from 100 to limit runtime of tests
MAX_SAMPLES = 10  # Reduced from 100 to limit runtime of tests

# COMMAND ----------

spark_rstr_test_suite = FunctionTestSuite()


@spark_rstr_test_suite.add_test
def test_specific_length():
  rstr = SparkRstr()
  assert_matches(r"^A{5}$", rstr.rstr("A", 5))

  
@spark_rstr_test_suite.add_test
def test_length_range():
  rstr = SparkRstr()
  assert_matches(r"^A{11,20}$", rstr.rstr("A", 11, 20))

  
@spark_rstr_test_suite.add_test
def test_custom_alphabet():
  rstr = SparkRstr()
  assert_matches(r"^A{1,10}$", rstr.rstr("AA"))

  
@spark_rstr_test_suite.add_test
def test_alphabet_as_list():
  rstr = SparkRstr()
  assert_matches(r"^A{1,10}$", rstr.rstr(["A", "A"]))

  
@spark_rstr_test_suite.add_test
def test_include():
  rstr = SparkRstr()
  assert_matches(r"^[ABC]*@[ABC]*$", rstr.rstr("ABC", include="@"))

  
@spark_rstr_test_suite.add_test
def test_include_specific_length():
  '''
  Verify including characters doesn't make the string longer than intended.
  '''
  rstr = SparkRstr()
  assert_matches(r"^[ABC@]{5}$", rstr.rstr("ABC", 5, include="@"))

  
@spark_rstr_test_suite.add_test
def test_exclude():
  rstr = SparkRstr()
  df = spark.range(100)
  col = rstr.rstr(r"ABC", exclude="C")
  assert df.filter(F.length(F.regexp_extract(col, r".*[C].*", 1)) > 0).first() is None

  
@spark_rstr_test_suite.add_test
def test_include_as_list():
  rstr = SparkRstr()
  assert_matches(r"^[ABC]*@[ABC]*$", rstr.rstr(r"ABC", include=["@"]))

  
@spark_rstr_test_suite.add_test
def test_exclude_as_list():
  rstr = SparkRstr()            
  df = spark.range(100)
  col = rstr.rstr(r"ABC", exclude=["C"])
  assert df.filter(F.length(F.regexp_extract(col, r".*[C].*", 1)) > 0).first() is None

  
spark_rstr_test_suite.run()

# COMMAND ----------

digits_test_suite = FunctionTestSuite()


@digits_test_suite.add_test
def test_all_digits():
  rstr = SparkRstr()
  assert_matches(r'^\d{1,10}$', rstr.digits())


@digits_test_suite.add_test
def test_digits_include():
  rstr = SparkRstr()
  assert_matches(r'^\d*@\d*$', rstr.digits(include='@'))


@digits_test_suite.add_test
def test_digits_exclude():
  rstr = SparkRstr()
  df = spark.range(100)
  col = rstr.digits(exclude='5').alias('value')
  assert df.filter(F.length(F.regexp_extract(col, ".*[5].*", 1)) > 0).first() is None
  
  
digits_test_suite.run()

# COMMAND ----------

non_digits_test_suite = FunctionTestSuite()


@non_digits_test_suite.add_test
def test_nondigits():
  rstr = SparkRstr()
  assert_matches(r'^\D{1,10}$', rstr.nondigits())


@non_digits_test_suite.add_test
def test_nondigits_include():
  rstr = SparkRstr()
  assert_matches(r'^\D*@\D*$', rstr.nondigits(include='@'))


@non_digits_test_suite.add_test
def test_nondigits_exclude():
  rstr = SparkRstr()
  df = spark.range(100)
  col = rstr.nondigits(exclude='A')
  assert df.filter(F.length(F.regexp_extract(col, r".*[A].*", 1)) > 0).first() is None

      
non_digits_test_suite.run()

# COMMAND ----------

letters_test_suite = FunctionTestSuite()


@letters_test_suite.add_test
def test_letters():
  rstr = SparkRstr()
  assert_matches(r'^[a-zA-Z]{1,10}$', rstr.letters())


@letters_test_suite.add_test
def test_letters_include():
  rstr = SparkRstr()
  assert_matches(r'^[a-zA-Z]*@[a-zA-Z]*$', rstr.letters(include='@'))


@letters_test_suite.add_test
def test_letters_exclude():
  rstr = SparkRstr()
  df = spark.range(100)
  col = rstr.letters(exclude='A')
  assert df.filter(F.length(F.regexp_extract(col, ".*[A].*", 1)) > 0).first() is None

  
letters_test_suite.run()

# COMMAND ----------

unambiguous_test_suite = FunctionTestSuite()


@unambiguous_test_suite.add_test
def test_unambiguous():
  rstr = SparkRstr()
  assert_matches('^[a-km-zA-HJ-NP-Z2-9]{1,10}$', rstr.unambiguous())

  
@unambiguous_test_suite.add_test
def test_unambiguous_include():
  rstr = SparkRstr()
  assert_matches('^[a-km-zA-HJ-NP-Z2-9@]{1,10}$', rstr.unambiguous(include='@'))

  
@unambiguous_test_suite.add_test
def test_unambiguous_exclude():
  rstr = SparkRstr()
  df = spark.range(100)
  col = rstr.letters(exclude='A').alias('value')
  assert df.filter(F.length(F.regexp_extract(col, ".*[A].*", 1)) > 0).first() is None
  
    
unambiguous_test_suite.run()

# COMMAND ----------

custom_alphabets_test_suite = FunctionTestSuite()


@custom_alphabets_test_suite.add_test
def test_alphabet_at_instantiation():
  rs = SparkRstr(vowels='AEIOU')
  assert_matches('^[AEIOU]{1,10}$', rs.vowels())

  
@custom_alphabets_test_suite.add_test
def test_add_alphabet():
  rs = SparkRstr()
  rs.add_alphabet('evens', '02468')
  assert_matches('^[02468]{1,10}$', rs.evens())
  

custom_alphabets_test_suite.run()

# COMMAND ----------

spark_xeger_test_suite = FunctionTestSuite()


@spark_xeger_test_suite.add_test
def test_literals():
    rstr = SparkRstr()
    pattern = r'foo'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_dot():
    '''
    Verify that the dot character doesn't produce newlines.
    See: https://bitbucket.org/leapfrogdevelopment/rstr/issue/1/
    '''
    rstr = SparkRstr()
    pattern = r'.+'
    df = spark.range(100)
    df = df.select(rstr.xeger(pattern).alias('value'))
    for row in df.collect():  
        assert re.match(pattern, row['value'])


@spark_xeger_test_suite.add_test
def test_digit():
    rstr = SparkRstr()
    pattern = r'\d'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_nondigits():
    rstr = SparkRstr()
    pattern = r'\D'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_literal_with_repeat():
    rstr = SparkRstr()
    pattern = r'A{3}'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_literal_with_range_repeat():
    rstr = SparkRstr()
    pattern = r'A{2, 5}'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_word():
    rstr = SparkRstr()
    pattern = r'\w'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_nonword():
    rstr = SparkRstr()
    pattern = r'\W'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_or():
    rstr = SparkRstr()
    pattern = r'foo|bar'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_or_with_subpattern():
    rstr = SparkRstr()
    pattern = r'(foo|bar)'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_range():
    rstr = SparkRstr()
    pattern = r'[A-F]'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_character_group():
    rstr = SparkRstr()
    pattern = r'[ABC]'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_carot():
    rstr = SparkRstr()
    pattern = r'^foo'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_dollarsign():
    rstr = SparkRstr()
    pattern = r'foo$'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_not_literal():
    rstr = SparkRstr()
    pattern = r'[^a]'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_negation_group():
    rstr = SparkRstr()
    pattern = r'[^AEIOU]'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_lookahead():
    rstr = SparkRstr()
    pattern = r'foo(?=bar)'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_lookbehind():
    rstr = SparkRstr()
    pattern = r'(?<=foo)bar'
    df = spark.createDataFrame([[1]], 'x: int')
    value = df.select(rstr.xeger(pattern).alias('value')).first()['value']
    errmsg = '{} does not match {}'.format(value, pattern)
    assert re.search(pattern, value), errmsg


@spark_xeger_test_suite.add_test
def test_backreference():
    rstr = SparkRstr()
    pattern = r'(foo|bar)baz\1'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_zero_or_more_greedy():
    rstr = SparkRstr()
    pattern = r'a*'
    assert_matches(pattern, rstr.xeger(pattern))


@spark_xeger_test_suite.add_test
def test_zero_or_more_non_greedy():
    rstr = SparkRstr()
    pattern = r'a*?'
    assert_matches(pattern, rstr.xeger(pattern))
    
    
spark_xeger_test_suite.run()

# COMMAND ----------

