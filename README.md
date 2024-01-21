# dftxt

A Python library for a simple DataFrame text file format that facilitates easier
specification of a Pandas and Polars DataFrame in a human-readable text format for
use in testing and where source data is small and human managed. By, example here's
what the format would look like:

```
Name      Planet    Numeral  Mean Radius (km)  Discovery Year  Discoverer
          &&cat              &&float           &&Int
Moon      Earth     I        1738              None            None
Phobos    Mars      I        11.267            1877            Hall
Deimos    Mars      II       6.2               1877            Hall
Io        Jupiter   I        1821              1610            Galileo
Europa    Jupiter   II       1560              1610            Galileo
Ganymede  Jupiter   III      2634              1610            Galileo
Callisto  Jupiter   IV       2410              1610            Galileo
Amalthea  Jupiter   V        83.5              1892            Barnard
Himalia   Jupiter   VI       69.8              1904            Perrine
Mimas     Saturn    I        198.2             1789            Herschel
```

This is a fixed-width file format that uses two+ spaces separating column names to
define the width of each column.

The benefits of the format are:

## 1. Preserves DataFrame Structure

Most importantly, this format retains the necessary information to reload the DataFrame
in an identical fashion as the file was specified. This includes data types, column
ordering, and indexing (Pandas only as Polars has no index). In testing, it should be
possible to use `(pandas|polars).testing.assert_frame_equal()` on a loaded DataFrame
without any transformation when read from a file.

For example,

```
sku           price_usd  originally_released_on  product_name
&&int_index   &decimal   &date
109456        119.99     2023-07-09              Fancy Socks
450213        24.49      2020-11-12              Simple Socks
90210         299.99     1998-03-28              LA Heartthrob Socks
```

## 2. Human Friendly

The format is easy to read and modify by humans and requires little to no machine
characters in its specification. Whitespace is used as the delimiter - specifically
2+ spaces between column names - which also serves to align columns for easy
readability. Quoting and escaping are rarely needed as a result.

## 3. Diff/Code Review Friendly

TODO
