# Databricks notebook source
# Source code: yanai repo (DPS) - path: src/dsp/common/relational.py
# Example usage: yanai repo (DPS) - path: src/dsp/datasets/definitions/mhsds_v5/submission_constants.py
from abc import ABC, abstractmethod
from itertools import groupby
from typing import Set, Tuple, FrozenSet, Any, Generator, Optional, Callable, Dict

from pyspark.sql import Column


class Field:
    """
    Datatype representing a field of a relational table
    """

    def __init__(
            self, name: str, data_type: type, uid: str = None, data_dictionary_name: str = None,
            primary: bool = False, idb_element_name: str = None, foreign: 'TableField' = None
    ):
        """
        Args:
            name (str): The name of the field
            data_type (type): The daatype of values stored in this field
            uid (str): The field unique identifier
            primary (bool): Whether this field is part of the primary key for the containing table
            idb_element_name (str): idb element Name
            foreign (TableField): The corresponding field from a foreign table if this field forms part of a foreign key
        """
        self._name = name
        self._data_type = data_type
        self._uid = uid
        self._data_dictionary_name = data_dictionary_name
        self._primary = primary
        self._foreign = foreign
        self._idb_element_name = idb_element_name or name

    @property
    def name(self) -> str:
        """
        Returns:
            str: The name of the field
        """
        return self._name

    @property
    def uid(self) -> str:
        """
        Returns:
            str: The uid of the field
        """
        return self._uid

    @property
    def idb_element_name(self) -> str:
        """
        Returns:
            str: The idb_element_name of the field
        """
        return self._idb_element_name

    @property
    def data_dictionary_name(self) -> Optional[str]:
        """
        Returns:
            str: The data dictionary name of the field
        """
        return self._data_dictionary_name

    @property
    def data_type(self) -> type:
        """
        Returns:
            type: The type of the field
        """
        return self._data_type

    @property
    def primary(self) -> bool:
        """
        Returns:
            bool: Whether the field is part of the primary key
        """
        return self._primary

    @property
    def foreign(self) -> Optional['TableField']:
        """
        Returns:
            Optional[TableField]: The corresponding field from a foreign table if this field forms part of a foreign key
        """
        return self._foreign

    def __eq__(self, other: Any) -> bool:
        return other is self \
               or (isinstance(other, Field)
                   and self.name == other.name
                   and self.data_type == other.data_type
                   and self.primary == other.primary
                   and self.foreign == other.foreign)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, Field):
            raise NotImplementedError
        if self.name != other.name:
            return self.name < other.name
        if self.primary != other.primary:
            return self.primary < other.primary

        if self.foreign is None:
            return other.foreign is not None

        if other.foreign is None:
            return False

        return self.foreign < other.foreign

    def __hash__(self):
        return hash((self._name, self._data_type, self._primary, self._foreign))

    def __str__(self):
        return self._name

    def __repr__(self):
        return "Field(name={name}, data_type={data_type}, primary={primary}, foreign={foreign})".format(
            name=repr(self.name), data_type=repr(self.data_type), primary=repr(self.primary),
            foreign=repr(self.foreign))


class TableBase(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Returns:
            str: The canonical name of this table
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def alias(self) -> Optional[str]:
        """
        Returns:
            Optional[str]: The alias applied to this table, if applicable
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def qualifier(self) -> str:
        """
        Returns:
            str: Qualifier for accessing fields of this table; either the table alias, if applicable, else the table
                name
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def fields(self) -> Tuple[Field, ...]:
        """
        Returns:
            FrozenSet[Field]: Fields of the table, with no enrichment for the containing table
        """
        raise NotImplementedError

    @property
    def qualified_fields(self) -> Generator['TableField', None, None]:
        """
        Returns:
            Generator[TableField, None, None]: The fields of this table in a qualified form
        """
        for field in self.fields:
            yield TableField(self, field)

    @abstractmethod
    def apply_alias(self, alias: str) -> 'TableBase':
        """
        Apply an alias to this table

        Args:
            alias (str): The alias by which to refer to this table

        Returns:
            TableBase: An aliased representation of this table
        """
        raise NotImplementedError

    @abstractmethod
    def has_field(self, field_name: str, case_sensitive: bool = True) -> bool:
        """
        Args:
            field_name (str): The name of the field to check for
            case_sensitive (bool): Whether to consider case when comparing column names

        Returns:
            bool: Whether this table has a field with the given name
        """
        raise NotImplementedError

    @abstractmethod
    def get_field(self, field_name: str, case_sensitive: bool = True) -> Field:
        """
        Retrieve a field by name from this table

        Args:
            field_name (str): The field to be retrieved
            case_sensitive (bool): Whether the field name needs to be matched in a case-sensitive fashion

        Returns:
            TableField: The field of this table with matching name

        Raises:
            ValueError: If no field exists for this table with the given name
        """
        raise NotImplementedError

    def get_qualified_field(self, field_name: str, case_sensitive: bool = True) -> 'TableField':
        return TableField(self, self.get_field(field_name, case_sensitive=case_sensitive))

    def related_tables(self) -> Generator['Table', None, None]:
        """
        Get tables related to this one by foreign keys

        Returns:
            Generator[Table, None, None]: Generator of tables for which this table contains foreign keys
        """
        reported_tables = set()  # type: Set[TableBase]
        for field in self.fields:
            if field.foreign and field.foreign.table not in reported_tables:
                yield field.foreign.table
                reported_tables.add(field.foreign.table)

    def get_field_path(self, field_name: str, case_sensitive: bool = True, header_table: 'Table' = None) -> \
            Tuple['TableField', FrozenSet['JoinChain']]:
        """
        Resolve a field from this table, joining to available related tables as necessary

        Args:
            field_name (str): The field to resolve
            case_sensitive (bool): Whether to treat the given field in a case-sensitive fashion
            header_table (Table): The header table for this dataset, which should contain only a single row and can be
            referred to by any other table

        Returns:
            Tuple[TableField, FrozenSet[JoinChain]]: A tuple whose first element is the resolved field, and whose second
            is the joins required from this table to retrieve the field

        Raises:
            ValueError: If the given field name cannot be found in this table or any related table
        """

        def recurse(table: Table) -> Tuple[Optional[TableField], Set[JoinChain]]:
            if table.has_field(field_name, case_sensitive):
                return table.get_qualified_field(field_name, case_sensitive), set()

            for related_table in table.related_tables():
                foreign_field, join_chain = recurse(related_table)
                if foreign_field is not None:
                    return foreign_field, {JoinChain(table.get_join_parameters(related_table), *join_chain)}

            return None, set()

        field, required_joins = recurse(self)

        if field is not None:
            return field, frozenset(required_joins)

        if header_table is not None:
            field, required_joins = recurse(header_table)
            if field is not None:
                return field, frozenset({JoinChain(JoinParameters(self, header_table), *required_joins)})

        raise ValueError("Unable to resolve field {field_name} from table {table_name}".format(field_name=field_name,
                                                                                               table_name=self._name))

    def get_join_parameters(self, foreign_table: 'Table',
                            conditions: Callable[[], Tuple[Column, ...]] = tuple) -> 'JoinParameters':
        """
        Get the parameters required to join to a related table

        Args:
            foreign_table (Table): The table to join to
            conditions (Callable[[], Tuple[Column, ...]]): Conditions to apply to the right hand table prior to joining

        Returns:
            JoinParameters: Parameters describing how to join to the requested table
        Raises:
            ValueError: If the given table is not related to this table
        """
        join_keys = {(field, field.foreign) for field in self.qualified_fields
                     if field.foreign and field.foreign.table == foreign_table}
        if not join_keys:
            raise ValueError("{} is not a related table to {}".format(foreign_table.name, self.name))
        return JoinParameters(self, foreign_table, *join_keys, conditions=conditions)

    def get_required_joins(self, foreign_table: 'Table', header_table: 'Table' = None,
                           metadata_table: 'Table' = None) -> Set['JoinChain']:
        """
        Retrieve any joins required between this table and a given foreign table

        Args:
            foreign_table (Table): The table to join to
            header_table (Table): The header table for this dataset that can be joined to any other table
            metadata_table (Table): The metadata table for this dataset that can be joined to any other table

        Returns:
            Set[JoinChain]: The joins required to reach the foreign table from this instance

        Raises:
            ValueError: If the given foreign table cannot be reached from this table
        """
        if foreign_table == self:
            return set()

        if foreign_table == header_table or foreign_table == metadata_table:
            return {JoinChain(JoinParameters(self, foreign_table))}

        def recurse(left_table: Table, right_table: Table) -> Optional[Set[JoinChain]]:
            left_relationships = sorted(left_table.related_tables(), reverse=False)
            if right_table in left_relationships:
                return {JoinChain(left_table.get_join_parameters(right_table))}

            for related_table in left_relationships:
                child_join = recurse(related_table, right_table)
                if child_join is not None:
                    return {JoinChain(left_table.get_join_parameters(related_table), *child_join)}

            return None

        join_chain = recurse(self, foreign_table)
        if join_chain is not None:
            return join_chain

        raise ValueError("Table " + foreign_table.name + " cannot be reached from " + self.name)

    def __getitem__(self, item: str) -> 'TableField':
        try:
            return self.get_qualified_field(item)
        except ValueError as e:
            raise KeyError from e

    def __hash__(self) -> int:
        return hash((self.name, self.qualifier, self.fields))

    def __eq__(self, other: Any) -> bool:
        return other is self \
               or (isinstance(other, TableBase)
                   and self.name == other.name
                   and self.qualifier == other.qualifier
                   and self.fields == other.fields)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, TableBase):
            raise NotImplementedError
        if self.name != other.name:
            return self.name < other.name
        if self.qualifier != other.qualifier:
            return self.qualifier < other.qualifier
        return self.fields < other.fields


class Table(TableBase):
    """
    Datatype representing a relational table
    """

    def __init__(self, name: str, *fields: Field):
        """
        Args:
            name (str): The name of this table
            *fields (Field): The fields of this table
        """
        self._name = name
        self._fields = fields
        self._fields_by_name = {field.name: field for field in fields}

        # Ensure given fields all have unique names
        assert len(self._fields) == len(self._fields_by_name)

    @property
    def name(self) -> str:
        return self._name

    @property
    def alias(self) -> Optional[str]:
        return None

    @property
    def qualifier(self) -> str:
        return self.name

    @property
    def fields(self) -> Tuple[Field, ...]:
        return self._fields

    @property
    def uids_to_names(self) -> Dict[str, str]:
        return {f.uid: f.name for f in self._fields}

    def apply_alias(self, alias: str) -> 'AliasedTable':
        return AliasedTable(self, alias)

    def has_field(self, field_name: str, case_sensitive: bool = True) -> bool:
        if case_sensitive:
            return field_name in self._fields_by_name

        normalized_field_name = field_name.lower()
        return any(normalized_field_name == candidate_field.name.lower() for candidate_field in self._fields)

    def get_field(self, field_name: str, case_sensitive: bool = True) -> Field:
        if case_sensitive:
            try:
                matching_field = self._fields_by_name[field_name]  # type: Optional[Field]
            except KeyError:
                matching_field = None
        else:
            normalized_field_name = field_name.lower()
            matching_field = None
            for field in self._fields:
                if field.name.lower() == normalized_field_name:
                    matching_field = field
                    break

        if matching_field is None:
            raise ValueError(f"No field with name {field_name} in table {self.name}")

        return matching_field

    def __str__(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return "Table(name={name}, fields={fields})".format(name=repr(self._name), fields=repr(self._fields))


class AliasedTable(TableBase):
    """
    Datatype representing a table with a custom alias
    """

    def __init__(self, table: Table, alias: str):
        """
        Args:
            table (Table): The actual table being aliased
            alias (str): The alias by which to refer to the table
        """
        self._table = table
        self._alias = alias

    @property
    def name(self) -> str:
        return self._table.name

    @property
    def alias(self) -> Optional[str]:
        return self._alias

    @property
    def qualifier(self) -> str:
        return self.alias

    @property
    def fields(self) -> Tuple[Field, ...]:
        return self._table.fields

    def apply_alias(self, alias: str) -> 'AliasedTable':
        return AliasedTable(self._table, alias)

    def has_field(self, field_name: str, case_sensitive: bool = True) -> bool:
        return self._table.has_field(field_name, case_sensitive=case_sensitive)

    def get_field(self, field_name: str, case_sensitive: bool = True) -> Field:
        return self._table.get_field(field_name, case_sensitive=case_sensitive)

    def __str__(self) -> str:
        return "{} AS {}".format(self.name, self.alias)

    def __repr__(self) -> str:
        return "AliasedTable(table={table}, alias={alias})".format(table=repr(self._table), alias=repr(self._alias))


class TableField:
    """
    Datatype representing a field enriched by data of its containing table
    """

    def __init__(self, table: TableBase, field: Field):
        """
        Args:
            table (TableBase): The table containing the field
            field (Field): The enriched field
        """
        self._table = table
        self._field = field

    @property
    def table(self) -> TableBase:
        """
        Returns:
            Table: The table containing the field
        """
        return self._table

    @property
    def field(self) -> Field:
        """
        Returns:
            Field: The field being represented
        """
        return self._field

    @property
    def name(self) -> str:
        """
        Returns:
            str: The name of the field
        """
        return self._field.name

    @property
    def qualified_name(self) -> str:
        """
        Returns:
            str: The name of the field, qualified with the name of the table
        """
        return "{}.{}".format(self._table.qualifier, self._field.name)

    @property
    def uid(self) -> Optional[str]:
        """
        Returns:
            str: The uid of the field
        """
        return self._field.uid

    @property
    def data_dictionary_name(self) -> Optional[str]:
        """
        Returns:
            str: The data dictionary name of the field
        """
        return self._field.data_dictionary_name

    @property
    def data_type(self) -> type:
        """
        Returns:
            type: The data type of the field
        """
        return self._field.data_type

    @property
    def primary(self) -> bool:
        """
        Returns:
            bool: Whether the field is part of the primary key of the table
        """
        return self._field.primary

    @property
    def foreign(self) -> Optional['TableField']:
        """
        Returns:
            Optional[TableField]: The corresponding field from a foreign table if this field forms part of a foreign key
        """
        return self._field.foreign

    def __eq__(self, other: Any) -> bool:
        return other is self \
               or (isinstance(other, TableField)
                   and self.table == other.table
                   and self.field == other.field)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, TableField):
            raise NotImplementedError
        if self.table != other.table:
            return other.table < other.table
        return self.field < other.field

    def __hash__(self) -> int:
        return hash((self._table, self._field))

    def __str__(self) -> str:
        return self.qualified_name

    def __repr__(self) -> str:
        return "TableField(table={table}, field={field})".format(table=repr(self.table), field=repr(self.field))


class JoinParameters:
    """
    Parameters for joining two Spark dataframes registered to a session
    """

    def __init__(
            self,
            left_table: TableBase,
            right_table: TableBase,
            *join_keys: Tuple[TableField, TableField],
            conditions: Callable[[], Tuple[Column, ...]] = tuple
    ):
        """
        Args:
            left_table (Table): The table on the left hand side of the join
            right_table (Table): The table on the right hand side of the join
            *join_keys (Tuple[TableField, TableField]): A mapping of keys from the left hand side to the right hand side
            to be joined
            conditions (Callable[[], Tuple[Column, ...]]): A list of conditions to apply to the right hand table before
                the join
            rhs_alias (str): The alias to apply to the right hand table prior to the join
        """
        for left_key, right_key in join_keys:
            assert left_key.table == left_table and right_key.table == right_table, \
                "Cannot join {} to {} with keys {} -> {}".format(left_table.name, right_table.name,
                                                                 left_key.name, right_key.name)

        self._left_table = left_table
        self._right_table = right_table
        self._join_keys = frozenset(join_keys)
        self._conditions = conditions

    @property
    def left_table(self) -> Table:
        return self._left_table

    @property
    def right_table(self) -> Table:
        return self._right_table

    @property
    def join_keys(self) -> FrozenSet[Tuple[TableField, TableField]]:
        return self._join_keys

    @property
    def conditions(self) -> Callable[[], Tuple[Column, ...]]:
        return self._conditions

    def invert(self, conditions: Callable[[], Tuple[Column, ...]] = tuple) -> 'JoinParameters':
        """
        Retrieve a JoinParameters instance describing a join where the left hand side of this instance's join has become
        the right hand side and vice versa

        Args:
            conditions (Callable[[], Tuple[Column, ...]]): Conditions to apply to the right hand side of the inverted
                join, as we cannot programmatically invert our own condition
            rhs_alias (str): The alias to apply to the right hand table prior to the join

        Returns:
            JoinParameters: The parameters required for the inverted join
        """
        return JoinParameters(self.right_table, self.left_table,
                              *((right_key, left_key) for left_key, right_key in self.join_keys),
                              conditions=conditions)

    def __eq__(self, other: Any) -> bool:
        return other is self \
               or (isinstance(other, JoinParameters)
                   and self.left_table == other.left_table
                   and self.right_table == other.right_table
                   and self.join_keys == other.join_keys)

    def __lt__(self, other: 'JoinParameters') -> bool:
        if self.left_table != other.left_table:
            return self.left_table < other.left_table
        if self.right_table != other.right_table:
            return self.right_table < other.right_table
        return sorted(self.join_keys) < sorted(other.join_keys)

    def __hash__(self):
        return hash((self.left_table, self.right_table, self.join_keys))

    def __str__(self):
        as_str = str(self.left_table) + " x " + str(self.right_table)
        if self.join_keys:
            as_str += ": " + "; ".join(left_field.qualified_name + " x " + right_field.qualified_name
                                       for left_field, right_field in self._join_keys)

        return as_str

    def __repr__(self):
        return "JoinParameters(" \
               "left_table={left_table}, right_table={right_table}, " \
               "join_keys={join_keys}, conditions={conditions}" \
               ")" \
            .format(left_table=repr(self.left_table), right_table=repr(self.right_table),
                    join_keys=repr(self.join_keys), conditions=repr(self.conditions))


class JoinChain:
    """
    Data structure representing a sequence of joins, where required data is retrieved through a series of relationships
    """

    def __init__(self, join_parameters: JoinParameters, *child_joins: 'JoinChain'):
        """
        Args:
            join_parameters (JoinParameters): Parameters for the primary join described by this instance
            *child_joins (JoinChain): Descriptors of joins to be performed subsequent to the primary join
        """
        self._join_parameters = join_parameters
        self._child_joins = frozenset(child_joins)

    @property
    def join_parameters(self) -> JoinParameters:
        """
        Returns:
            JoinParameters: The parameters of the primary join described by this chain
        """
        return self._join_parameters

    @property
    def child_joins(self) -> FrozenSet['JoinChain']:
        """
        Returns:
            FrozenSet[JoinChain]: Collection of joins to be performed following primary join
        """
        return self._child_joins

    def __eq__(self, other: Any) -> bool:
        return other is self \
               or (isinstance(other, JoinChain)
                   and self.join_parameters == other.join_parameters
                   and self.child_joins == other.child_joins)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, JoinChain):
            raise NotImplementedError

        if self.join_parameters != other.join_parameters:
            return self.join_parameters < other.join_parameters

        return sorted(self.child_joins) < sorted(other.child_joins)

    def __hash__(self) -> int:
        return hash((self.join_parameters, self.child_joins))

    def __str__(self) -> str:
        return str(self._join_parameters) + " -> (" + ", ".join(
            (str(child_join) for child_join in self._child_joins)) + ")"

    def __repr__(self) -> str:
        return "JoinChain(join_parameters={join_parameters}, *{child_joins})".format(
            join_parameters=repr(self._join_parameters), child_joins=repr(tuple(self._child_joins)))

    @staticmethod
    def combine(*chains: 'JoinChain') -> FrozenSet['JoinChain']:
        """
        Combine a collection of join chains into the minimum number of equivalent joins

        Args:
            *chains (JoinChain): The chains to be combined

        Returns:
            FrozenSet[JoinChain]: A minimum set of joins required
        """

        def key_fn(chain: JoinChain):
            return chain.join_parameters

        return frozenset({JoinChain(join_parameters, *JoinChain.combine(
            *{child_join for join_chain in join_chains for child_join in join_chain.child_joins}))
                          for join_parameters, join_chains
                          in groupby(sorted(chains, key=key_fn), key=key_fn)})


def _absolute_field(table_name: str, field_name: str) -> str:
    absolute_field_prefix = table_name + "."
    if field_name.startswith(absolute_field_prefix):
        return field_name

    return absolute_field_prefix + field_name