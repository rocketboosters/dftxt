# dftxt

A Python library for a simple DataFrame text file format that facilitates easier
specification of a Pandas and Polars DataFrame in a reliable, human-readable text
format for use in testing and where source data is small and human managed. Ultimately,
the goal of this project is to make a DataFrame transformation function test as easy
as:

```python
def test_my_transformation():
    """Should transform source DataFrame into the expected output."""
    data_frames = dftxt.read_all_to_pandas("./test_data.dftxt")
    observed = my_transformation(data_frames.source)
    pandas.testing.assert_frame_equal(observed, data_frames.expected)
```

by allowing one to express the attributes, structure and data that constitute a
DataFrame within a text file format and avoid having to post-process loaded data.
Here's an example showing what the basic dftxt format looks like:

```
Name      Planet      Numeral  Mean Radius (km)  Discovery Year  Discoverer
          &dtype=cat           &dtype=float      &dtype=Int
Moon      Earth       I        1738              None            None
Phobos    Mars        I        11.267            1877            Hall
Deimos    Mars        II       6.2               1877            Hall
Io        Jupiter     I        1821              1610            Galileo
Europa    Jupiter     II       1560              1610            Galileo
Ganymede  Jupiter     III      2634              1610            Galileo
Callisto  Jupiter     IV       2410              1610            Galileo
Amalthea  Jupiter     V        83.5              1892            Barnard
Himalia   Jupiter     VI       69.8              1904            Perrine
Mimas     Saturn      I        198.2             1789            Herschel
```

This is a fixed-width file format that uses two+ spaces separating column names to
define the width of each column.

## Quick Start

One of the best ways to learn the dftxt is to create a Pandas/Polar DataFrame and save
it to a file or string and see what the output looks like.

```python
import dftxt
import polars as pl

df = pl.DataFrame([
  {"character": "Jay Gatsby", "book": "The Great Gatsby", "year": 1925},
  {"character": "Clarissa Dalloway", "book": "Mrs. Dalloway", "year": 1925},
  {"character": "Toad", "book": "The Wind & the Willow", "year": 1906},
])

# Could write to a file if you prefer:
# dftxt.write("./example.dftxt", df)
# But here we'll just print the serialized string:
print(dftxt.writes(df))
```

which would print out:

```
character          book                   year
                                          &dtype=int
Jay Gatsby         The Great Gatsby       1925
Clarissa Dalloway  Mrs. Dalloway          1925
Toad               The Wind & the Willow  1906
```

## Benefits

The benefits of the dftxt DataFrame serialization format include:

### 1. Preserves DataFrame Structure

Most importantly, this format retains the necessary information to reload the DataFrame
in an identical fashion as the file was specified. This includes data types, column
ordering, and indexing (Pandas only as Polars has no index). In testing, it should be
possible to use `(pandas|polars).testing.assert_frame_equal()` on a loaded DataFrame
without any transformation when read from a file.

For example,

```
sku         price_usd       originally_released_on  product_name
&int_index  &dtype=decimal  &dtype=date
109456      119.99          2023-07-09              Fancy Socks
450213      24.49           2020-11-12              Simple Socks
90210       299.99          1998-03-28              LA Heartthrob Socks
```

In this example the:

- `sku` column will be loaded as the DataFrame's index (Pandas only) and the values in
  the column treated as integers.
- `price_usd` column will be loaded as `decimal.Decimal` or `polars.Decimal`
  depending on the type of DataFrame.
- `originally_released_on` column will be loaded as `datetime.Date` values.
- `product_name` values will be loaded as strings.

The order of the rows and columns are preserved when loaded as well. All of this avoids
having to process the DataFrame in order to achieve this configuration as one would have
to do with other formats, e.g. CSV.

### 2. Human Friendly

The format is easy to read and modify by humans and requires little to no machine
characters in its specification. Whitespace is used as the delimiter - specifically
2+ spaces between column names - which also serves to align columns for easy
readability.

#### Quoting

Quoting and escaping are rarely needed as a result. Here's an example
showing three string columns, which require no quoting:

```
Name           Birth Month  Favorite Movie
Jane Doe       February     Back to the Future
John Doe       April        Harry Potter & the Goblet of Fire
Anna Johnson   November     Frozen
Steve Simpson  June         Avengers: Infinity War
```

Spaces in the column names and in the data values are not an issue for the format
because the 2+ spaces between columns are what's required to identify the columns.
Quoting is needed in cases:

1. The column name contains 2+ spaces, e.g. `"Hello  World"`.
2. The column name or value starts or ends with a space , e.g. `" foo "`.
3. The columns name or value ends with a backslash.

#### Column Wrapping

Additionally, long values can be broken up over multiple lines on a per-column basis
using a backslash end character like in Python. This makes it possible to keep long
strings from bloating the width of the data and hurting readability. For example,

```
Play            Quotation                                    Act    Scene
                                                             &&int  &&int
Hamlet          To be, or not to be: that is the question    3      1

As You Like It  All the world's a stage, and all the men \   2      7
                and women merely players. They have their \
                exits and their entrances; And one man in \
                his time plays many parts.

Romeo & Juliet  Romeo, Romeo! Wherefore art thou Romeo?      2      2

Richard III     Now is the winter of our discontent          1      1

Macbeth         Is this a dagger which I see before me, \    2      1
                the handle toward my hand?
```

Note that blank lines between rows here is optional and was included to emphasize the
wrapped lines in the `Quotation` column for the second and last rows.

#### Comments

Importantly, this format also supports commenting, using the standard Python line
comment, which begins a line of any indentation with a `#` sign. _Note that inline
comments are not supported because `#` is a common-enough character in data values that
it would confuse. A commented dftxt file looks something like this:

```
# This is a heavily commented example showing that comments can be used throughout a
# dftxt file. Very useful for documenting data for tests with the nuances and reasoning
# behind the test data.
# The data is source from:
# https://www.irs.gov/individuals/international-taxpayers/yearly-average-currency-exchange-rates
Country         Currency  2023-01-01  2022-01-01  2021-01-01  2020-01-01  2019-01-01
                          # We're using Decimals here because of currency accuracy needs.
                          &&decimal   &&decimal   &&decimal   &&decimal   &&decimal
                          # We want the column names to be loaded in as dates.
                          &ntype=date &ntype=date &ntype=date &ntype=date &ntype=date
                                      # The global pandemic strained the Argentine economy,
                                      # which led to soaring inflation that remained going
                                      # into 2024.
Argentina       Peso      296.154     130.792     95.098      70.635      48.192
                                      # Despite economic issues of its own, Brazil did not
                                      # see the same inflationary pressures of its neighbor.
Brazil          Real      4.994       5.165       5.395       5.151       3.946

Canada          Dollar    1.350       1.301       1.254       1.341       1.327
Cayman Islands  Dollar    0.833       0.833       0.833       0.833       0.833

Australia       Dollar    1.506       1.442       1.332       1.452       1.439
China           Yuan      7.075       6.730       6.452       6.900       6.910

Euro Zone       Euro      0.924       0.951       0.846       0.877       0.893

# The values here are yearly average currency exchange rates converting into USD.
```

### 3. Diff/Code Review Friendly

The benefits of the dftxt file format that make it human-friendly are also what make it
friendly for code reviewing and display diffs.

#### DataFrame Wrapping

Additionally, DataFrames can be separated into multiple blocks within a file to keep
wide datasets manageable and easy to read in diffs as well as preventing small changes
from having big impacts on file changes.

For example, this:

```
Name           Birth Month
Jane Doe       February
John Doe       April
Anna Johnson   November
Steve Simpson  June


Favorite Movie
Back to the Future
Harry Potter & the Goblet of Fire
Frozen
Avengers: Infinity War
```

is identical to this:


```
Name           Birth Month  Favorite Movie
Jane Doe       February     Back to the Future
John Doe       April        Harry Potter & the Goblet of Fire
Anna Johnson   November     Frozen
Steve Simpson  June         Avengers: Infinity War
```

The 2+ blank lines in a dftxt file indicate that the what follows are additional
column data for the same DataFrame. It's also possible to repeat columns - often this
will be an index or primary key - to make it easy to track lines in different blocks of
the wrapped DataFrame. The example above could be written as:


```
Name           Birth Month
Jane Doe       February
John Doe       April
Anna Johnson   November
Steve Simpson  June


Name           Favorite Movie
&repeat
Jane Doe       Back to the Future
John Doe       Harry Potter & the Goblet of Fire
Anna Johnson   Frozen
Steve Simpson  Avengers: Infinity War
```

Here the `&repeat` modifier on the second appearance of the `Name` column indicates
that this column is a repeat of one already in the DataFrame and should not be loaded
again. It exists purely for human-readability.

It's also possible to have an index column exist only in the file and have it never
be included in the loaded result. Continuing the example from above, this might look
something like:


```
ID        Name           Birth Month
&exclude
1         Jane Doe       February
2         John Doe       April
3         Anna Johnson   November
4         Steve Simpson  June


ID        Favorite Movie
&exclude
1         Back to the Future
2         Harry Potter & the Goblet of Fire
3         Frozen
4         Avengers: Infinity War
```

Here the `ID` column exists only in the file and will be excluded from the loading
process because of the `&exclude` column modifier.

### 4. Flexibility

The dftxt has additional flexibility in a key ways.

#### Multiple DataFrames

First, the dftxt format allows for specifying multiple DataFrames in a single file.
This can be used in a number of ways, but the most common one is to include all
DataFrames for a test in a single location for coherence. DataFrames within a file
are separated by a line that begins with 3+ dashes with a blank both before and after
it. This looks like:

```
Name           Birth Month  Favorite Movie
Jane Doe       February     Back to the Future
John Doe       April        Harry Potter & the Goblet of Fire
Anna Johnson   November     Frozen
Steve Simpson  June         Avengers: Infinity War

---

Movie                               Budget ($M)  Box Office ($M)
                                    &dtype=int   &dtype=decimal
Avengers: Infinity War              400          2052.0
Back to the Future                  19           388.8
Frozen                              150          1280.0
Harry Potter & the Goblet of Fire   150          896.8

---

Name           Birth Month  Favorite Movie                     Budget ($M)  Box Office ($M)
                                                               &dtype=int   &dtype=decimal
Jane Doe       February     Back to the Future                 19           388.8
John Doe       April        Harry Potter & the Goblet of Fire  150          896.8
Anna Johnson   November     Frozen                             150          1280.0
Steve Simpson  June         Avengers: Infinity War             400          2052.0
```

To load and use this file would look something like this:

```python
import pandas.testing
import dftxt

data_frames = dftxt.read_all_to_pandas("./example.dftxt")
combined = (
    frames[0]
    .merge(
        frames[1],
        how="left",
        left_on="Favorite Movie",
        right_on="Movie",
    )
    .drop(columns=["Movie"])
)
pandas.testing.assert_frame_equal(combined, data_frames[2])
```

Notice how the DataFrames are access by the indexed order from the file. It is also
possible to name the DataFrames in the file and access them by a name instead. This
would look something like this:

```
--- people ---

Name           Birth Month  Favorite Movie
Jane Doe       February     Back to the Future
John Doe       April        Harry Potter & the Goblet of Fire
Anna Johnson   November     Frozen
Steve Simpson  June         Avengers: Infinity War

--- movies ---

Movie                               Budget ($M)  Box Office ($M)
                                    &dtype=int   &dtype=decimal
Avengers: Infinity War              400          2052.0
Back to the Future                  19           388.8
Frozen                              150          1280.0
Harry Potter & the Goblet of Fire   150          896.8

--- expected ---

Name           Birth Month  Favorite Movie                     Budget ($M)  Box Office ($M)
                                                               &dtype=int   &dtype=decimal
Jane Doe       February     Back to the Future                 19           388.8
John Doe       April        Harry Potter & the Goblet of Fire  150          896.8
Anna Johnson   November     Frozen                             150          1280.0
Steve Simpson  June         Avengers: Infinity War             400          2052.0
```

and would be loaded and accessed like this:

```python
data_frames = dftxt.read_all_to_pandas("./example.dftxt")
combined = data_frames.people.merge(
    data_frames.movies,
    how="left",
    left_on="Favorite Movie",
    right_on="Movie",
).drop(columns=["Movie"])
pandas.testing.assert_frame_equal(combined, data_frames.expected)
```

Names must be valid Python variables. Also, the trailing `---` in the named example is
optional. It could also have been `--- people` instead of `--- people ---`.

#### Column Filtering

It is also possible add filters to columns to load different columns under different
circumstances. There are two types of filters `if` and `if_not`. An `if` filter will
only be included if the specified filter value is specified when loading. An `if_not`
filter will be excluded if the filter is present. These provide a lot of flexibility,
but can be very useful when testing mapping transformations without having to specify
data multiple times.

Continuing from the example in the previous "Multiple DataFrames" section, the expected
DataFrame could be omitted and the combined columns added to the people DataFrame with
filters. Also, in this example we'll drop the `Birth Month` column in the expected to
show if_not filtering as well. Here's what the file would look like:

```
--- people ---

Name           Birth Month  Favorite Movie                     Budget ($M)  Box Office ($M)
               &-expected                                      &dtype=int   &dtype=decimal
                                                               &+expected   &+expected
Jane Doe       February     Back to the Future                 19           388.8
John Doe       April        Harry Potter & the Goblet of Fire  150          896.8
Anna Johnson   November     Frozen                             150          1280.0
Steve Simpson  June         Avengers: Infinity War             400          2052.0

--- movies ---

Movie                               Budget ($M)  Box Office ($M)
                                    &dtype=int   &dtype=decimal
Avengers: Infinity War              400          2052.0
Back to the Future                  19           388.8
Frozen                              150          1280.0
Harry Potter & the Goblet of Fire   150          896.8
```

The if filters can be specified as `&if=expected` and the if not filters specified as
`&if_not=expected`. However, here the shorthand is used, which is `&+expected` and
`&-expected` respectively. In this case the `Birth Month` column will be loaded unless
the read call specifies the `exclude` filter. In contrast the `Budget ($M)` and
`Box Office ($M)` columns will only be included if the read call specifies the `exclude`
filter. In practice, this would look like:

```python
frames = dftxt.read_all_to_pandas("./example.dftxt")
expected_frames = dftxt.read_all_to_pandas("./example.dftxt", filters=["expected"])
combined = frames.people.merge(
    frames.movies,
    how="left",
    left_on="Favorite Movie",
    right_on="Movie",
).drop(columns=["Movie", "Birth Month"])
pandas.testing.assert_frame_equal(combined, expected_frames.people)
```

It is possible to specify multiple if and not if filters to a single column if
desirable. In those cases a column will be included when any of the if filters are
present. The not if filters take precedence and the column will be omitted if any of
the filters match the not if filters condition.
