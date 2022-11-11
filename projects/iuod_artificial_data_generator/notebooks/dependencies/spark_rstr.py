# Databricks notebook source
import sys
import functools
import random

from pyspark.sql import types as T, functions as F

# COMMAND ----------

# MAGIC %run ./rstr

# COMMAND ----------

# MAGIC %run ./random

# COMMAND ----------

def array_lit(arr: list[Union[str, int, float]]) -> Column:
  """Create a constant column of array type from the given list.
  
  Parameters
  ----------
  arr: list[Union[str, int, float]]
    Constant list to populate rows
  
  Returns
  -------
  pyspark.sql.Column
    Constant column where every row equals the provided list
  """
  return F.array(*map(F.lit, arr))

# COMMAND ----------

# Wrapper for Xeger class from rstr/xeger.py

# The maximum number of characters generated for a pattern containing a *+ token
SPARK_STAR_PLUS_LIMIT = 100


class SparkXeger(Xeger):
  """PySpark wrapper for Xeger
  """
  def __init__(self):
    super().__init__()
    self._cache = CacheMap()

    self._cases = {
      'literal': lambda x: array_lit([chr(x)]),
      'not_literal': lambda x: self._random.choice(string.printable.replace(chr(x), '')),
      'at': lambda x: array_lit(['']),
      'in': self._handle_in,  
      'any': lambda x: self.printable(1, exclude='\n'),
      'range': lambda x: array_lit([chr(i) for i in range(x[0], x[1] + 1)]),
      'category': lambda x: array_lit(list(self._categories[x]())),
      'branch': self._handle_branch,
      'subpattern': self._handle_group,
      'assert': lambda x: F.concat_ws('', *list(map(self._handle_state, x[1]))),
      'assert_not': lambda x: F.lit(''),
      'groupref': lambda x: self._cache[x],
      'min_repeat': lambda x: self._handle_repeat(*x),
      'max_repeat': lambda x: self._handle_repeat(*x),
      'negate': lambda x: False,
    }
        
  def xeger(self, string_or_regex):
    result = super().xeger(string_or_regex)
    return result.alias(f'xeger({string_or_regex})')

  def _build_string(self, parsed):
    newstr = F.concat_ws('', *list(map(self._handle_state, parsed)))
    return newstr
  
  def _handle_state(self, state):
    return super()._handle_state(state)

  def _handle_group(self, value):
    result = F.concat_ws('', *list(map(self._handle_state, value[-1])))
    if value[0]:
      self._cache[value[0]] = result
    return result

  def _handle_in(self, value):
    first_candidate = self._handle_state(value[0])
    rest_candidates = functools.reduce(F.array_union, map(self._handle_state, value[1:]), F.array([]).cast(T.ArrayType(T.StringType())))

    if first_candidate is False:
      candidates = F.array_except(array_lit(list(set(string.printable))), rest_candidates)
    else:
      candidates = F.array_union(first_candidate, rest_candidates)

    index = self._random.randint(F.lit(0), F.size(candidates) - F.lit(1))
    return candidates[index]

  def _handle_repeat(self, start_range, end_range, value):
    result = []
    end_range = min((end_range, SPARK_STAR_PLUS_LIMIT))

    # The elements in the array defined below have lengths from start_range to end_range
    # Each element is the result of repeated calls to self._handle_state with the elements of value
    # The zeroth element is a string of length start_range, the second a string of length start_range+1, etc up to end_range
    full_string_col = F.concat_ws('', *[F.concat_ws('', *[self._handle_state(v) for v in value]) for _ in range(end_range + 1)])
    sub_string_col = F.array([F.substring(full_string_col, 1, n) for n in range(start_range, end_range + 1)])

    # Pick an element from the array of substrings
    index = self._random.randint(0, F.size(sub_string_col) - F.lit(1))
    result = sub_string_col[index]
    return result
  
  def _handle_branch(self, x):
    values_col = F.array([F.concat_ws('', *[self._handle_state(i) for i in j]) for j in x[1]])
    index_col = self._random.randint(0, F.size(values_col) - F.lit(1))
    return F.array([values_col[index_col]])
  
  
class CacheMap(object):
  """Provides dict-like interface to MapType column for use by Xeger.
  """
  def __init__(self):
    self._cache = F.create_map()
    
  def clear(self):
    self._cache = F.create_map()
    
  def __setitem__(self, key, value):
    self._cache = F.map_concat(self._cache, F.create_map(F.lit(key), F.lit(value)))
    
  def __getitem__(self, key):
    return self._cache[key]

# COMMAND ----------

# Wrappers for classes in rstr/rstr_base.py

MAX_SAMPLES = 100


class SparkRstrBase(RstrBase):
  """PySpark wrapper for RstrBase
  """
  def sample_wr(self, population, k, kmin=None, kmax=None):
    '''Samples k random elements (with replacement) from a population'''
    kmin = kmin if kmin is not None else 1
    kmax = kmax if kmax is not None else 10
    return F.array([F.array([self._random.choice(population) for _ in range(n)]) for n in range(kmin, kmax)])[k-kmin]

  def rstr(self, alphabet, start_range=None, end_range=None, include='', exclude=''):
    '''Generate a random string containing elements from 'alphabet'
    By default, rstr() will return a string between 1 and 10 characters.
    You can specify a second argument to get an exact length of string.
    If you want a string in a range of lengths, specify the start and end
    of that range as the second and third arguments.
    If you want to make certain that particular characters appear in the
    generated string, specify them as "include".
    If you want to *prevent* certain characters from appearing, pass them
    as 'exclude'.
    '''
    popul = [char for char in list(alphabet) if char not in list(exclude)]

    if end_range is None:
        if start_range is None:
            start_range, end_range = (1, 10)
        else:
            k = start_range

    if end_range:
        k = self._random.randint(start_range, end_range)
    else:
        end_range = start_range + 1
        
    # Make sure we don't generate too long a string
    # when adding 'include' to it:
    offset = len(include)
    kmin = start_range - offset
    kmax = end_range - offset
    k = k - F.lit(offset)

    result = F.concat(self.sample_wr(popul, k, kmin=kmin, kmax=kmax), array_lit(list(include)))
    result = self._random.shuffle(result)
    return F.concat_ws('', result)


class SparkRstr(SparkRstrBase, SparkXeger):
  """PySpark wrapper for Rstr
  """
  def __init__(self, _random=spark_random, **alphabets):
    super().__init__(_random=_random, **alphabets)    

# COMMAND ----------

