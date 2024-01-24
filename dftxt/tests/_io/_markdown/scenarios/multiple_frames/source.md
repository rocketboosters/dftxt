# Example

This is a scenario demonstrating how a DataFrame can be specified within a markdown
file using fenced code blocks.

The data can be specified over multiple fenced code blocks, which will be concatenated
together when creating the results. Only code blocks that include `df` or `dftxt` as
the language identifier will be considered. This allows including fenced code blocks of
other types without issue.

For example, here we start defining a dftxt DataFrame. Note that in this file we're
going to load multiple DataFrames, so we give them names as the second argument (space
separated) on the fenced code block opening line.

_Note: The DataFrame name specified in the code block isn't visible in the rendered
Markdown, so it can be useful to use a header or some other indication of the DataFrame
if there are multiple in the file._

## People DataFrame

```df people
Name           Birth Month  Favorite Movie
Jane Doe       February     Back to the Future
John Doe       April        Harry Potter & the Goblet of Fire
```

This starts the **people** DataFrame. We can continue adding rows in another block that
specifies also must specified it is for the **people** DataFrame.

```df people
Anna Johnson   November     Frozen
Steve Simpson  June         Avengers: Infinity War
```

If we want to wrap the DataFrame to prevent it from becoming too wide in the display,
we can do that as well. There are two ways to do this. The first would be to start
with 2+ empty lines in the block. However, that can look odd in a Markdown context,
so there's a more succinct way to specify this by adding `...` to the end of the
DataFrame name. _Note: if only a single DataFrame is specified and it is not named, the
first argument would be `...` on its own._

Here we wrap the **people** DataFrame to add additional
columns. Instead of starting the block with 2+ empty lines,
which would indicate the wrapping in a dftxt file, we append
`...` to the end of the DataFrame's name.

```df people...
Name            Favorite Book
&repeat
Jane Doe        Emma
John Doe        Harry Potter & the Goblet of Fire
Anna Johnson    The Girl with the Dragon Tattoo
Steve Simpson   The Three Musketeers
```

## Books DataFrame

Now we can start a new DataFrame in a new block with a
different name.

```df books
Name                               Published  Page Count
                                   &&int      &&int
The Three Musketeers               1844       700
The Girl with the Dragon Tattoo    2005       544
Harry Potter & the Goblet of Fire  2000       636
Emma                               1816       1036
```

And that's all there is to it. The contents of the fenced code blocks act as embedded
dftxt format. So anything done in a dftxt file can be used here as well.
