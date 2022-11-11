# Databricks notebook source
# MAGIC %run ../rstr

# COMMAND ----------

import re
import unittest
import random

# from rstr.rstr_base import Rstr


def assert_matches(pattern, value):
    errmsg = '{} does not match {}'.format(value, pattern)
    assert re.match(pattern, value), errmsg


class TestRstr(unittest.TestCase):
    def setUp(self):
        self.rs = Rstr()

    def test_specific_length(self):
        assert_matches('^A{5}$', self.rs.rstr('A', 5))

    def test_length_range(self):
        assert_matches('^A{11,20}$', self.rs.rstr('A', 11, 20))

    def test_custom_alphabet(self):
        assert_matches('^A{1,10}$', self.rs.rstr('AA'))

    def test_alphabet_as_list(self):
        assert_matches('^A{1,10}$', self.rs.rstr(['A', 'A']))

    def test_include(self):
        assert_matches('^[ABC]*@[ABC]*$', self.rs.rstr('ABC', include='@'))

    def test_include_specific_length(self):
        '''
        Verify including characters doesn't make the string longer than intended.
        '''
        assert_matches('^[ABC@]{5}$', self.rs.rstr('ABC', 5, include='@'))

    def test_exclude(self):
        for _ in range(0, 100):
            assert 'C' not in self.rs.rstr('ABC', exclude='C')

    def test_include_as_list(self):
        assert_matches('^[ABC]*@[ABC]*$', self.rs.rstr('ABC', include=['@']))

    def test_exclude_as_list(self):
        for _ in range(0, 100):
            assert 'C' not in self.rs.rstr('ABC', exclude=['C'])


class TestSystemRandom(TestRstr):
    def setUp(self):
        self.rs = Rstr(random.SystemRandom())


class TestDigits(unittest.TestCase):
    def setUp(self):
        self.rs = Rstr()

    def test_all_digits(self):
        assert_matches(r'^\d{1,10}$', self.rs.digits())

    def test_digits_include(self):
        assert_matches(r'^\d*@\d*$', self.rs.digits(include='@'))

    def test_digits_exclude(self):
        for _ in range(0, 100):
            assert '5' not in self.rs.digits(exclude='5')


class TestNondigits(unittest.TestCase):
    def setUp(self):
        self.rs = Rstr()

    def test_nondigits(self):
        assert_matches(r'^\D{1,10}$', self.rs.nondigits())

    def test_nondigits_include(self):
        assert_matches(r'^\D*@\D*$', self.rs.nondigits(include='@'))

    def test_nondigits_exclude(self):
        for _ in range(0, 100):
            assert 'A' not in self.rs.nondigits(exclude='A')


class TestLetters(unittest.TestCase):
    def setUp(self):
        self.rs = Rstr()

    def test_letters(self):
        assert_matches(r'^[a-zA-Z]{1,10}$', self.rs.letters())

    def test_letters_include(self):
        assert_matches(r'^[a-zA-Z]*@[a-zA-Z]*$', self.rs.letters(include='@'))

    def test_letters_exclude(self):
        for _ in range(0, 100):
            assert 'A' not in self.rs.letters(exclude='A')


class TestUnambiguous(unittest.TestCase):
    def setUp(self):
        self.rs = Rstr()

    def test_unambiguous(self):
        assert_matches('^[a-km-zA-HJ-NP-Z2-9]{1,10}$', self.rs.unambiguous())

    def test_unambiguous_include(self):
        assert_matches('^[a-km-zA-HJ-NP-Z2-9@]{1,10}$', self.rs.unambiguous(include='@'))

    def test_unambiguous_exclude(self):
        for _ in range(0, 100):
            assert 'A' not in self.rs.unambiguous(exclude='A')


class TestCustomAlphabets(unittest.TestCase):
    def test_alphabet_at_instantiation(self):
        rs = Rstr(vowels='AEIOU')
        assert_matches('^[AEIOU]{1,10}$', rs.vowels())

    def test_add_alphabet(self):
        rs = Rstr()
        rs.add_alphabet('evens', '02468')
        assert_matches('^[02468]{1,10}$', rs.evens())

# COMMAND ----------

# Load the tests from each test case
load_tests = unittest.defaultTestLoader.loadTestsFromTestCase
test_cases = [
  TestRstr, 
  TestSystemRandom, 
  TestDigits, 
  TestNondigits, 
  TestLetters,
  TestUnambiguous,
  TestCustomAlphabets
]
test_suites = []

# Iteratively create test suites by loading tests
for test_case in test_cases:
  test_suites.append(load_tests(test_case))

# Run the tests
suite = unittest.TestSuite(tests=test_suites)
runner = unittest.TextTestRunner()
runner.run(suite)

# COMMAND ----------

# rstr/tests/test_xeger.py

import re
import unittest

# from rstr.rstr_base import Rstr


class TestXeger(unittest.TestCase):
    def setUp(self):
        self.rs = Rstr()

    def test_literals(self):
        pattern = r'foo'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_dot(self):
        '''
        Verify that the dot character doesn't produce newlines.
        See: https://bitbucket.org/leapfrogdevelopment/rstr/issue/1/
        '''
        pattern = r'.+'
        for i in range(100):
            assert re.match(pattern, self.rs.xeger(pattern))

    def test_digit(self):
        pattern = r'\d'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_nondigits(self):
        pattern = r'\D'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_literal_with_repeat(self):
        pattern = r'A{3}'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_literal_with_range_repeat(self):
        pattern = r'A{2, 5}'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_word(self):
        pattern = r'\w'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_nonword(self):
        pattern = r'\W'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_or(self):
        pattern = r'foo|bar'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_or_with_subpattern(self):
        pattern = r'(foo|bar)'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_range(self):
        pattern = r'[A-F]'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_character_group(self):
        pattern = r'[ABC]'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_carot(self):
        pattern = r'^foo'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_dollarsign(self):
        pattern = r'foo$'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_not_literal(self):
        pattern = r'[^a]'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_negation_group(self):
        pattern = r'[^AEIOU]'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_lookahead(self):
        pattern = r'foo(?=bar)'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_lookbehind(self):
        pattern = r'(?<=foo)bar'
        assert re.search(pattern, self.rs.xeger(pattern))

    def test_backreference(self):
        pattern = r'(foo|bar)baz\1'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_zero_or_more_greedy(self):
        pattern = r'a*'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_zero_or_more_non_greedy(self):
        pattern = r'a*?'
        assert re.match(pattern, self.rs.xeger(pattern))

# COMMAND ----------

# Run the tests
suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestXeger)
runner = unittest.TextTestRunner()
runner.run(suite)

# COMMAND ----------

