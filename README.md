# Pysheet README
## Overview
Pysheet is your best companion to Excel for data management. It can read and write to a spreadsheet, consolidate columns and merge spreadsheets together. It allows you to query for information thus turning your Excel sheet into a little database. It can be used both as a python library and as a command-line tool and supports concurrent access control.

## Quick Start
1. Install [_texttable_](https://pypi.python.org/pypi/texttable):

        pip install texttable

2. Download _pysheet.py_ from this repository
3. Run the following commands in the folder where you downloaded _pysheet.py_:

        ./pysheet.py -o helloworld.csv --write 1 A Pysheet 2 B your 3 C best 4 D companion 5 E to 6 F Excel -v
        ./pysheet.py -d helloworld.csv --columns 4 2 1 3 5 6

Output:

    ID |     D     |  B   |    A    |  C   | E  |   F   
    ===+===========+======+=========+======+====+======
    1  |           |      | Pysheet |      |    |       
    2  |           | your |         |      |    |       
    3  |           |      |         | best |    |       
    4  | companion |      |         |      |    |       
    5  |           |      |         |      | to |       
    6  |           |      |         |      |    | Excel 

_helloworld.csv_ in Excel:

![helloworld.csv](https://raw.github.com/isthisthat/Pysheet/master/test/helloworld.png)

## _Real-life_ Demo
For a real-life demo and a more in-depth look at Pysheet, let's look at an example from the field of genetics.
Download the following files and place them in the same directory as pysheet.py:

* A catalogue of genes implicated in cancer [cancer_gene_census.tsv](http://cancer.sanger.ac.uk/cancergenome/data/cancer_gene_census.tsv)
* Zebrafish genes that have a correspondence (ortholog) in human [ortho_2013.05.15.txt](http://zfin.org/downloads/file/ortho.txt?tsv=2013.05.15)

We will try to produce a list of human cancer genes which also exist in Zebrafish (as you might imagine, since humans are more comple organisms, not all human genes will exist in Zebrafish. Similarly, since humans don't have gills, some Zebrafish genes will not be present in humans). The aim is to study these cancer genes in Zebrafish in the laboratory (a controlled environment), rather than on humans.

Let's first get a printout of our two files. We know that the files are tab-delimited so we will use the `--delimiter`/`-D` argument:

    -D'\t'

From the directory that contains pysheet.py and the two above files type:

    ./pysheet.py -d ortho_2013.05.15.txt -D'\t' -k | head

Output:

     ZFIN ID   |    ZFIN    |   Entrez   | Human Gene |   Entrez   |            
               |   Symbol   | Zebrafish  |   Symbol   | Human Gene |            
               |            |  Gene ID   |            |     ID     |            
    ===========+============+============+============+============+===========
    ZDB-GENE-0 | dlc        | 30120      | DLL3       | 10683      |            
    00125-4    |            |            |            |            |            
    ZDB-GENE-0 | pbx4       | 30728      | PBX3       | 5090       |            
    00201-18   |            |            |            |            |            
    ZDB-GENE-0 | kal1a      | 30630      | KAL1       | 3730       |            
    00201-9    |            |            |            |            |            

Here we see an empty column at the end. This is because the headers in the file had an extra _tab_ at the end so Pysheet read this as an empty column.

The next file is quite large so we will just display the 3rd and 4th column (the first column is displayed by default).
We start counting from 0 so we'll use the `--columns`/`-k` argument:

    -k 2-3

Let's check the file:

    ./pysheet.py -d cancer_gene_census.tsv -D'\t' -k 2-3 | head

Output:

      Symbol    | GeneID | Chr 
    ============+========+====
    ABL1        | 25     | 9   
    ABL2        | 27     | 1   
    ACSL3       | 2181   | 2   
    AF15Q14     | 57082  | 15  
    AF1Q        | 10962  | 1   
    AF3p21      | 51517  | 3   
    AF5q31      | 27125  | 5   
    AKAP9       | 10142  | 7   

We wish to join these two files by Gene Symbol. In the first file, this is the 4th column, whereas in the second file, this is the 1st column. Therefore we need to specify that the IDs we wish to merge on appear in the 4th column of the first file. We need to use the `--dataIdCol`/`-i` argument:

    -i3

Let's merge these files in memory and print the resulting headers. We'll use the `--mergeSheet`/`-m`, `--mergeDelim`/`-M` and `--printHeaders`/`-H` arguments in the command:

    ./pysheet.py -d pheno_2013.05.15.txt -i3 -D'\t' -m cancer_gene_census.tsv -M '\t' -H

The output should be:

    0 ZFIN ID
    1 ZFIN Symbol
    2 Entrez Zebrafish Gene ID
    3 Human Gene Symbol
    4 Entrez Human Gene ID
    5 
    6 Name
    7 GeneID
    8 Chr
    9 Chr Band
    10 Cancer Somatic Mut
    11 Cancer Germline Mut
    12 Tumour Types  (Somatic Mutations)
    13 Tumour Types (Germline Mutations)
    14 Cancer Syndrome
    15 Tissue Type
    16 Cancer Molecular Genetics
    17 Mutation Type
    18 Translocation Partner
    19 Other Germline Mut
    20 Other Syndrome/Disease

Let's manipulate the merged file a bit more before we save it. We would like to consolidate columns containg the keywords _Cancer_, _Mut_ or _Other_ under the column _Phenotype_. We'll use the `--clean`/`-C` argument:

    -C Phenotype Cancer Mut Other

Let's see the resulting headers:

    ./pysheet.py -d test/pheno_2013.05.15.txt -i3 -D'\t' -m test/cancer_gene_census.tsv -M '\t' -C Phenotype Cancer Mut Other -H

Output:

	0 ZFIN ID
	1 __Phenotype
	2 ZFIN Symbol
	3 Entrez Zebrafish Gene ID
	4 Human Gene Symbol
	5 Entrez Human Gene ID
	6 
	7 Name
	8 GeneID
	9 Chr
	10 Chr Band
	11 Tissue Type
	12 Translocation Partner

Notice that the consolidated columns that matched our keywords above, namely:

    10 Cancer Somatic Mut
    11 Cancer Germline Mut
    12 Tumour Types  (Somatic Mutations)
    13 Tumour Types (Germline Mutations)
    14 Cancer Syndrome
    16 Cancer Molecular Genetics
    17 Mutation Type
    19 Other Germline Mut
    20 Other Syndrome/Disease

Have been removed and replaced by:

	1 __Phenotype

The double underscore `__` in front of the header name indicates a "locked" header, i.e. do not use this header in further consolidations. But the header can still be called without the double underscore and is also printed without it as you will see below.
Let's visualise a few lines from this column:

    ./pysheet.py -d pheno_2013.05.15.txt -i3 -D'\t' -m cancer_gene_census.tsv -M '\t' -C Phenotype Cancer Mut Other -k Phenotype | head

Output:

	         Human Gene Symbol           |              Phenotype               
	=====================================+=====================================
	ABCA12                               |                                      
	ABCB6                                |                                      
	ABCC6                                |                                      
	ABCE1                                |                                      
	ABHD11                               |                                      
	ABL1                                 | yes;CML, ALL, T-ALL;Dom;T, Mis       
	ABL2                                 | yes;AML;Dom;T                        
	ABRA                                 |                                      

As you can see, this new column contains all values from the consolidated columns, delimited by `;`

Now we'd like to reshuffle columns a bit, so we'll use this ordering:

    -k 5 2 3 1 10

And finally output to a comma-separated file _cancer\_zebrafish.csv_. Use the command:

    ./pysheet.py -d pheno_2013.05.15.txt -D3 -l'\t' -m cancer_gene_census.tsv -D '\t' -C Phenotype Cancer Mut Other -k 5 2 3 1 10 -o cancer_zebrafish.csv

Let's print the headers of this file:

    ./pysheet.py -d cancer_zebrafish.csv -H

Output:

    0 Human Gene Symbol
    1 Entrez Human Gene ID
    2 ZFIN Symbol
    3 Entrez Zebrafish Gene ID
    4 Phenotype
    5 Chr Band

Great. That's a bit more manageable. Now let's print the gene IDs which were common in the two starting files. We'll use the `--query`/`-q` argument:

    -q 2
or

    -q 'ZFIN Symbol'

which implicitly means, "give me all IDs which have a non-blank value under the column _ZFIN Symbol_". Since all rows have a _Human Gene Symbol_ (that was the column which we used to merge the two files), the rows that additionally contain a _ZFIN Symbol_ represent the common genes between our two original files. Let's print them:

    ./pysheet.py -d cancer_zebrafish.csv -q 2 -v | head

Output:

    1234 INFO +++ Input sheet: /path/to/cancer_zebrafish.csv
    1234 INFO === Query '2' returned 1925 IDs..
    ABCA12
    ABCB6
    ABCC6
    ABCE1
    ABHD11
    ABRA
    ACD
    ACHE
    ACTA1
    ACTA2

So there are 1925 such cases. This additional information appears because we used the `--verbose`/`-v` argument.

>__TIP__: the number to the left of _INFO_ (here _1234_) is the process ID of this instance of pysheet. This information is useful when running multiple instances of pysheet in parallel (see `-L` option).

Let's say we are interested in genes involved in _Acute Myeloid Leukemia_ or _AML_. Let's now see the cases which contain _AML_ in the _Phenotype_ column which we created previously:

    ./pysheet.py -d cancer_zebrafish.csv -q 2 'Phenotype~AML'

We get 19 such genes:

    CBFA2T3
    CBFB
    ERG
    FUS
    GAS7
    GATA2
    GMPS
    HOXA13
    JAK2
    KIT
    KRAS
    MYH11
    NCOA2
    NPM1
    NUP98
    PDGFRB
    PTPN11
    RUNX1
    SBDS

Let's now say we wish to exclude gene _HOXA13_ because we think it's irrelevant. We'll use the special ___Exclude_ column for that. Let's use the `--write`/`-w` argument to the entry _yes_ under the ___Exclude_ column of our spreadsheet.

    ./pysheet.py -d cancer_zebrafish.csv -w HOXA13 __Exclude yes -o cancer_zebrafish.csv

And run the previous query again:

    ./pysheet.py -d cancer_zebrafish.csv -q 2 'Phenotype~AML'

Now _HOXA13_ is excluded from our list (but still present in our spreadsheet):

    CBFA2T3
    CBFB
    ERG
    FUS
    GAS7
    GATA2
    GMPS
    JAK2
    KIT
    KRAS
    MYH11
    NCOA2
    NPM1
    NUP98
    PDGFRB
    PTPN11
    RUNX1
    SBDS

We now have our gene list and can start our Zebrafish experiments!

Notice the `~` operator which we just used to filter our _Phenotype_ column. You can similarly use any of the following operators:

Operator | Description | Example
--- | --- | ---
`=` | equals | `-k Sex=F`
`>` | greater than | `-q Dose>0.5`
`<` | less than | `-k Age>20 Age<40`
`!` | not | `-q Timepoint!0`
`~` | contains | `-k Condition~oma`

Additionally, you may use `=UNIQUE` to get the first occurence of each item in a column, for example `-k Name=UNIQUE`


## Documentation
 

### Command-Line Help
Generated using:

    ./pysheet.py --help

Pasted below:

    usage: pysheet.py [-h] [--dataSheet CSV] [--dataDelim DELIMITER]
                      [--dataIdCol N] [--dataSkipCol N] [--lockFile [LOCKFILE]]
                      [--outSheet CSV] [--outDelim DELIMITER] [--outNoHeaders]
                      [--write [ID HEADER VALUE [ID HEADER VALUE ...]] | --read
                      [ID HEADER [ID HEADER ...]] | --remove
                      [ID HEADER [ID HEADER ...]]] [--mergeSheet CSV]
                      [--mergeDelim DELIMITER] [--mergeIdCol N] [--mergeSkipCol N]
                      [--consolidate [HEADER KEYWORD1 KEYWORD2 etc [HEADER KEYWORD1 KEYWORD2 etc ...]]]
                      [--clean [HEADER KEYWORD1 KEYWORD2 etc [HEADER KEYWORD1 KEYWORD2 etc ...]]]
                      [--columns [COLUMNS [COLUMNS ...]]]
                      [--query [QUERY [QUERY ...]]] [--printHeaders] [--version]
                      [--verbose]

    A library to read and write comma-separated (.csv) spreadsheets

    optional arguments:
      -h, --help            show this help message and exit
      --version, -V         show program's version number and exit
      --verbose, -v         verbosity level

    Input/Output:
      --dataSheet CSV, -d CSV
                            A delimited spreadsheet with unique IDs in the first
                            column (or use -i) and headers in the first row
      --dataDelim DELIMITER, -D DELIMITER
                            The delimiter of the input dataSheet. Default is comma
                            (,)
      --dataIdCol N, -i N   Column number (starting from 0) which contains the
                            unique IDs. Enter -1 for auto-generating column ids.
                            Default is 0 (1st column)
      --dataSkipCol N, -s N
                            Skip this number of rows from the top of the file
      --lockFile [LOCKFILE], -L [LOCKFILE]
                            Prevents parallel jobs from overwriting the dataSheet.
                            Use in cluster environments or asynchronous loops.
                            Optionally, specify a filename (default is
                            <dataSheet>.lock
      --outSheet CSV, -o CSV
                            Output filename (may include path)
      --outDelim DELIMITER, -O DELIMITER
                            The delimiter of the output Sheet. Default is comma
                            (,)
      --outNoHeaders, -nh   Don't output the header row at the top

    Read/Write:
      --write [ID HEADER VALUE [ID HEADER VALUE ...]], -w [ID HEADER VALUE [ID HEADER VALUE ...]]
                            Write new cells
      --read [ID HEADER [ID HEADER ...]], -r [ID HEADER [ID HEADER ...]]
                            Print value of cells to screen
      --remove [ID HEADER [ID HEADER ...]], -R [ID HEADER [ID HEADER ...]]
                            Remove cells

    Merge:
      --mergeSheet CSV, -m CSV
                            Merge another spreadsheet to this file (can be used
                            multiple times)
      --mergeDelim DELIMITER, -M DELIMITER
                            The delimiter of mergeSheet. Default is comma (,)
      --mergeIdCol N, -I N  Column number (starting from 0) which contains the
                            unique IDs of the mergeSheet. Default is 0
      --mergeSkipCol N, -S N
                            Skip this number of rows from the top of the file

    Consolidate:
      --consolidate [HEADER KEYWORD1 KEYWORD2 etc [HEADER KEYWORD1 KEYWORD2 etc ...]], -c [HEADER KEYWORD1 KEYWORD2 etc [HEADER KEYWORD1 KEYWORD2 etc ...]]
                            Consolidate columns according to keywords (can be used
                            multiple times)
      --clean [HEADER KEYWORD1 KEYWORD2 etc [HEADER KEYWORD1 KEYWORD2 etc ...]], -C [HEADER KEYWORD1 KEYWORD2 etc [HEADER KEYWORD1 KEYWORD2 etc ...]]
                            Consolidate and remove consolitated columns (can be
                            used multiple times)

    Query:
      --columns [COLUMNS [COLUMNS ...]], -k [COLUMNS [COLUMNS ...]]
                            Extracts specific columns from the dataSheet. e.g.
                            '1-3 Age'. Default is print all columns
      --query [QUERY [QUERY ...]], -q [QUERY [QUERY ...]]
                            Extracts IDs that meet a query. e.g. 'Age>25
                            Group=Normal Validated' (NOTE: will not return IDs
                            that have a non-blank entry in the special 'Exclude'
                            column)
      --printHeaders, -H    Prints out all column headers and their index

    Examples:
        pysheet.py -o mystudy.csv -w ID001 Age 38 ID002 Gender M
            creates a blank sheet and adds two cell entries to it. Saves it as ./mystudy.csv
        
        pysheet.py -d mystudy.csv -c Items store -C Price price -C Availability avail -q Availability Price>0.5
            consolidates Items across columns whose headers contain the keyword 'store'. Similarly for Price and Availability
            then prints all IDs of Items with Price>0.5 and non-blank Availability
        
        pysheet.py -d /path/table.txt -D'\t' -o ./test/mystudy.csv -k 5 1-3 -v
            reads a tab-delimited sheet and saves the columns 0 (assumed to be the IDs) 5,1,2,3 in csv format as ./test/mystudy.csv

        pysheet.py -d mystudy.csv -k
            prints the entire data sheet to screen

        pysheet.py -d mystudy.csv -R 01001 Status -o mystudy.csv
            removes the cell for ID '01001' under the 'Status' column

        pysheet.py -d results.csv -w iteration_$i Result $val -o results.csv -L
            adds a cell to the results sheet (locking the file before read/write access)

        pysheet.py -d table.txt -D '\t' -i -1 -k 2 3 1 -o table_subset.txt -O '\t' -nh
            rearranges the first 3 column of a tab-delimited file and saves it out without a header

### Pydoc
Generated using:

    import pydoc
    from pysheet import Pysheet
    pydoc.writedoc(Pysheet)

Available at [Pysheet.html](http://htmlpreview.github.io/?https://github.com/isthisthat/Pysheet/blob/master/Pysheet.html)

## TODO
* Allow indexing by more than one column (e.g. chromosome\_position\_allele)
* Allow creation of SubSheets, i.e. to extract certain rows and columns from a Pysheet
* Integrate version control (perhaps git) for disaster recovery
* Handle the case when the are duplicate IDs (and thus they overwrite each-other during loading)
* Submit this as a package to PyPi
* Please [let me know](https://github.com/isthisthat/Pysheet/issues) if you'd like to see more features!


## Help!
I hope you find pysheet useful. If you need more help, please [contact me](https://github.com/isthisthat)! I'd be happy to hear from you.  
Please submit feature requests and bug reports [via github](https://github.com/isthisthat/Pysheet/issues).

