"""
Adds generic querying for catalogue metadata.
"""

OPERATORS = {
    "eq": lambda c, v: c == v,
    "ne": lambda c, v: c != v,
    "gt": lambda c, v: c > v,
    "gte": lambda c, v: c >= v,
    "lt": lambda c, v: c < v,
    "lte": lambda c, v: c <= v,
    "contains": lambda c, v: c.ilike(f"%{v}%"),
    "startswith": lambda c, v: c.ilike(f"{v}%"),
    "endswith": lambda c, v: c.ilike(f"%{v}"),
    "in": lambda c, v: c.in_(v),
}


def convert_value(column, value: str):
    """
    Convert a query parameter string into the appropriate Python type
    based on a SQLAlchemy column definition.


    Parameters
    ----------
    column : sqlalchemy.Column
        SQLAlchemy column whose type will be used to determine the
        conversion target.
    value : str
        Raw query parameter value from the request.

    Returns
    -------
    Any
        Value converted to the column's Python type if possible.
        If conversion fails, the original string value is returned.

    Notes
    -----
    Supported boolean values interpreted as ``True`` are:

    - ``"true"``
    - ``"1"``
    - ``"yes"``

    All other values are interpreted as ``False`` when converting
    to a boolean column.
    """
    try:
        python_type = column.type.python_type

        if python_type is bool:
            return value.lower() in ("true", "1", "yes")

        return python_type(value)

    except Exception:  # pylint: disable=broad-exception-caught
        return value


class QueryBuilder:
    """
    Utility class for constructing SQLAlchemy queries from HTTP
    query parameters.

    Supported query patterns include:

    - ``field=value`` (equality)
    - ``field__gt=value`` (greater than)
    - ``field__lt=value`` (less than)
    - ``field__contains=value`` (substring search)
    - ``field__in=a,b,c`` (membership)

    Sorting and field selection are also supported:

    - ``sort=field`` (ascending)
    - ``sort=-field`` (descending)
    - ``fields=a,b,c`` (column selection)

    Parameters
    ----------
    model : sqlalchemy.orm.DeclarativeMeta
        SQLAlchemy model class used to validate field names and
        access column definitions.
    query_params : Mapping
        Request query parameters, typically ``request.query_params``
        from FastAPI.
    """

    def __init__(self, model, query_params):
        """
        Initialize the query builder.

        Parameters
        ----------
        model : sqlalchemy.orm.DeclarativeMeta
            SQLAlchemy model class used to construct the query.
        query_params : Mapping
            Query parameters from the incoming HTTP request.
        """
        self.model = model
        self.params = dict(query_params)
        self.valid_columns = set(model.__mapper__.c.keys())

    def apply_filters(self, query):
        """
        Apply filter expressions to a SQLAlchemy query.

        Query parameters are interpreted as filter conditions using
        the syntax ``field__operator=value``. If no operator is provided,
        equality is assumed.

        Parameters
        ----------
        query : sqlalchemy.orm.Query
            Base SQLAlchemy query to which filters will be applied.

        Returns
        -------
        sqlalchemy.orm.Query
            Query with filtering conditions applied.

        Notes
        -----
        Parameters named ``limit``, ``sort``, and ``fields`` are ignored
        here because they are handled separately.

        Invalid column names or unsupported operators are silently
        ignored to prevent query failures.
        """

        for key, value in self.params.items():

            if key in ("limit", "sort", "fields"):
                continue

            if "__" in key:
                field, op = key.split("__", 1)
            else:
                field, op = key, "eq"

            if field not in self.valid_columns:
                continue

            if op not in OPERATORS:
                continue

            column = getattr(self.model, field)

            if op == "in":
                value = [convert_value(column, v) for v in value.split(",")]
            else:
                value = convert_value(column, value)

            query = query.filter(OPERATORS[op](column, value))

        return query

    def apply_sort(self, query):
        """
        Apply ordering to a SQLAlchemy query.

        Sorting is controlled via the ``sort`` query parameter.
        Multiple fields may be specified as a comma-separated list.

        Examples
        --------
        ``sort=uploaded_at``

        ``sort=-uploaded_at``

        ``sort=-uploaded_at,version``

        Parameters
        ----------
        query : sqlalchemy.orm.Query
            SQLAlchemy query to which ordering will be applied.

        Returns
        -------
        sqlalchemy.orm.Query
            Query with ordering applied.

        Notes
        -----
        Prefixing a field with ``-`` indicates descending order.
        Fields not present in the model are ignored.
        """

        sort_param = self.params.get("sort")
        if not sort_param:
            return query

        fields = sort_param.split(",")

        for field in fields:

            descending = field.startswith("-")
            field = field.lstrip("-")

            if field not in self.valid_columns:
                continue

            column = getattr(self.model, field)

            query = query.order_by(column.desc() if descending else column.asc())

        return query

    def apply_limit(self, query):
        """
        Limit the number of rows returned.

        Example
        -------
        ``limit=5``

        Returns
        -------
        sqlalchemy.orm.Query
            Query with limit applied.
        """

        limit = int(self.params.get("limit", 100))

        return query.limit(limit)

    def get_selected_fields(self):
        """
        Determine which columns should be included in the response.

        The ``fields`` query parameter allows clients to restrict
        the returned columns to a subset of the model.

        Example
        -------
        ``fields=version,catalogue_name``

        Returns
        -------
        list[str] or None
            List of valid column names requested by the client.
            Returns ``None`` if no field selection was specified.
        """
        fields_param = self.params.get("fields")

        if not fields_param:
            return None

        fields = fields_param.split(",")

        return [f for f in fields if f in self.valid_columns]


def serialize_rows(rows, selected_fields=None):
    """
    Convert SQLAlchemy model instances into dictionaries suitable
    for JSON responses.

    Parameters
    ----------
    rows : list
        List of SQLAlchemy model instances.
    selected_fields : list[str], optional
        Optional list of column names to include in the output.
        If provided, only these fields will be returned.

    Returns
    -------
    list[dict]
        List of dictionaries representing the database rows.

    Notes
    -----
    The model instances must implement a ``columns_to_dict()`` method
    that returns a dictionary representation of the row.
    """
    results = []

    for row in rows:

        row_dict = row.columns_to_dict()

        if selected_fields:
            row_dict = {k: row_dict[k] for k in selected_fields}

        results.append(row_dict)

    return results
