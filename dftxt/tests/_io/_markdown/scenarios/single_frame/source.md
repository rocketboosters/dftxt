# Titanic Dataset

```dftxt
index    survived  pclass  sex     age      sibsp  parch  fare     embarked
&intidx  &&int     &&int           &&float  &&int  &&int  &&float
372      0         3       male    19.0     0      0      8.05     S
383      1         1       female  35.0     1      0      52.0     S
579      1         3       male    32.0     0      0      7.925    S
168      0         1       male    None     0      0      25.925   S
```

```df
577      1         1       female  39.0     1      0      55.9     S
705      0         2       male    39.0     0      0      26.0     S
649      1         3       female  23.0     0      0      7.55     S
349      0         3       male    42.0     0      0      8.6625   S
36       1         3       male    None     0      0      7.2292   C
```

```dftxt ...
index  class          who    adult_male  deck                      embark_town
&-     &&category:az         &&bool      &&category:A,B,C,D,1,F,G
372    Third          man    True        None                      Southampton
383    First          woman  False       None                      Southampton
```


```df
579    Third          man    True        None                      Southampton
168    First          man    True        None                      Southampton
577    First          woman  False       E                         Southampton
```


```df
705    Second         man    True        None                      Southampton
649    Third          woman  False       None                      Southampton
349    Third          man    True        None                      Southampton
36     Third          man    True        None                      Cherbourg
```

```df ...
index  alive  alone
&-            &&bool
372    no     True
383    yes    False
579    yes    True
168    no     True
```

```dftxt
577    yes    False
705    no     True
649    yes    True
349    no     True
36     yes    True
```
