# Pysheet README
[![Build Status](https://travis-ci.org/isthisthat/Pysheet.svg?branch=master)](https://travis-ci.org/isthisthat/Pysheet)

## Overview
Now on [pypi](https://pypi.python.org/pypi/pysheet)!
Pysheet is your best companion for data management. It can read and write to a delimited text file (spreadsheet), consolidate columns and merge spreadsheets together. It allows you to query for information thus turning your text file into a lightweight database. It can be used both as a python library (`from pysheet import Pysheet`) and as a command-line tool (`pysheet -h`) and supports concurrent access control for reading/writing to the same file in parallel.

## Quick Start
1. Install using pip:

    pip install pysheet

3. Try the following commands:

    pysheet -o helloworld.csv --write 1 A Pysheet 2 B your 3 C best 4 D companion 5 E to 6 F Excel -v
    pysheet -d helloworld.csv --columns 4 2 1 3 5 6

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
Download the following files:

* A catalogue of genes implicated in cancer [cancer_gene_census.tsv](http://cancer.sanger.ac.uk/cancergenome/assets/cancer_gene_census.tsv)
* Zebrafish genes that have a correspondence (ortholog) in human [ortho_2014.02.19.txt](http://zfin.org/downloads/file/ortho.txt?tsv=2014.02.19)

We will try to produce a list of human cancer genes which also exist in Zebrafish (as you might imagine, since humans are more comple organisms, not all human genes will exist in Zebrafish. Similarly, since humans don't have gills, some Zebrafish genes will not be present in humans). The aim is to study these cancer genes in Zebrafish in the laboratory (a controlled environment), rather than on humans.

Let's first get a printout of our two files. We know that the files are tab-delimited so we will use the `--delim`/`-D` argument:

    -D'\t'

From the directory that contains the two above files type:

    pysheet -d ortho_2014.02.19.txt -D'\t' -k | head

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

    pysheet -d cancer_gene_census.tsv -D'\t' -k 2-3 | head

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

We wish to join these two files by Gene Symbol. In the first file, this is the 4th column, whereas in the second file, this is the 1st column. Therefore we need to specify that the IDs we wish to merge on appear in the 4th column of the first file. We need to use the `--idCol`/`-i` argument:

    -i3

Let's merge these files in memory and print the resulting headers. We'll merge just by providing additional inputs ans then use the `--printHeaders`/`-H` argument in the command:

    pysheet -d ortho_2014.02.19.txt cancer_gene_census.tsv -i 3 0 -D '\t' '\t' -H

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

    pysheet -d ortho_2014.02.19.txt cancer_gene_census.tsv -i 3 0 -D '\t' '\t' -C Phenotype Cancer Mut Other -H

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

    pysheet -d ortho_2014.02.19.txt cancer_gene_census.tsv -i 3 0 -D '\t' '\t' -C Phenotype Cancer Mut Other -k Phenotype | head

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

    pysheet -d ortho_2014.02.19.txt cancer_gene_census.tsv -i 3 0 -D '\t' '\t' -C Phenotype Cancer Mut Other -k 5 2 3 1 10 -o cancer_zebrafish.csv

Let's print the headers of this file:

    pysheet -d cancer_zebrafish.csv -H

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

    pysheet -d cancer_zebrafish.csv -q 2 -v | head

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

    pysheet -d cancer_zebrafish.csv -q 2 'Phenotype~AML'

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

    pysheet -d cancer_zebrafish.csv -w HOXA13 __Exclude yes -o cancer_zebrafish.csv

And run the previous query again:

    pysheet -d cancer_zebrafish.csv -q 2 'Phenotype~AML'

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

    pysheet --help

Pasted below:

    usage: pysheet.py [-h] [--data [FILE [FILE ...]]] [--delim [CHAR [CHAR ...]]]
                      [--idCol [INT [INT ...]]] [--noHeader [Y|N [Y|N ...]]]
                      [--skipRow [INT [INT ...]]] [--skipCol [INT [INT ...]]]
                      [--trans [Y|N [Y|N ...]]] [--rstack] [--cstack] [--out FILE]
                      [--outDelim CHAR] [--outHeader [HEADER [HEADER ...]]]
                      [--outNoHeader] [--outTrans] [--outFname]
                      [--write [ID HEADER VALUE [ID HEADER VALUE ...]] | --read
                      [ID HEADER [ID HEADER ...]] | --remove
                      [ID HEADER [ID HEADER ...]]]
                      [--removeMissingRows | --removeMissingColumns]
                      [--lockFile [LOCKFILE]]
                      [--consolidate [HEADER KEYWORD1 KEYWORD2 etc [HEADER KEYWORD1 KEYWORD2 etc ...]]]
                      [--clean [HEADER KEYWORD1 KEYWORD2 etc [HEADER KEYWORD1 KEYWORD2 etc ...]]]
                      [--mode [append|overwrite|add|mean]]
                      [--columns [COLUMNS [COLUMNS ...]]]
                      [--query [QUERY [QUERY ...]]] [--printHeaders] [--version]
                      [--verbose]
    
    A library to read and write delimited text files
    
    optional arguments:
      -h, --help            show this help message and exit
      --version, -V         show program's version number and exit
      --verbose, -v         verbosity level
    
    Input:
      --data [FILE [FILE ...]], -d [FILE [FILE ...]]
                            Delimited text file with unique IDs in first column
                            (or use -i) and headers in first row. Or "stdin *"
      --delim [CHAR [CHAR ...]], -D [CHAR [CHAR ...]]
                            Delimiter of data. Default is auto-detect *
      --idCol [INT [INT ...]], -i [INT [INT ...]]
                            Column number (starting from 0) of unique IDs. Or "-1"
                            to auto-generate. Default is 0 (1st column) *
      --noHeader [Y|N [Y|N ...]], -n [Y|N [Y|N ...]]
                            Data file does not contain headers *
      --skipRow [INT [INT ...]], -s [INT [INT ...]]
                            Skip this number of rows from top of file *
      --skipCol [INT [INT ...]], -S [INT [INT ...]]
                            Skip columns from the right of the file *
      --trans [Y|N [Y|N ...]], -t [Y|N [Y|N ...]]
                            Read data transposed *
      --rstack, -rs         Stack input files by rows (regardless of headers)
      --cstack, -cs         Stack input files by columns (regardless of IDs)
    
    Output:
      --out FILE, -o FILE   Output filename (may include path). Or "stdout" *
      --outDelim CHAR, -O CHAR
                            Delimiter of output file. Default is comma
      --outHeader [HEADER [HEADER ...]], -OH [HEADER [HEADER ...]]
                            Replace output header with this list
      --outNoHeader, -N     Don't output header row at the top
      --outTrans, -T        Write output transposed
      --outFname, -OF       Add source filename as column
    
    Add/Remove:
      --write [ID HEADER VALUE [ID HEADER VALUE ...]], -w [ID HEADER VALUE [ID HEADER VALUE ...]]
                            Write new cells. Can use NONE for ID or HEADER *
      --read [ID HEADER [ID HEADER ...]], -r [ID HEADER [ID HEADER ...]]
                            Print value of cells *
      --remove [ID HEADER [ID HEADER ...]], -R [ID HEADER [ID HEADER ...]]
                            Remove cells *
      --removeMissingRows, -RR
                            Remove rows with missing values
      --removeMissingColumns, -RC
                            Remove columns with missing values
      --lockFile [LOCKFILE], -L [LOCKFILE]
                            Read/write lock to prevent parallel jobs from
                            overwriting the data. Use in asynchronous loops. You
                            may specify a filename (default is <out>.lock)
    
    Consolidate:
      --consolidate [HEADER KEYWORD1 KEYWORD2 etc [HEADER KEYWORD1 KEYWORD2 etc ...]], -c [HEADER KEYWORD1 KEYWORD2 etc [HEADER KEYWORD1 KEYWORD2 etc ...]]
                            Consolidate columns according to keywords *
      --clean [HEADER KEYWORD1 KEYWORD2 etc [HEADER KEYWORD1 KEYWORD2 etc ...]], -C [HEADER KEYWORD1 KEYWORD2 etc [HEADER KEYWORD1 KEYWORD2 etc ...]]
                            Consolidate and remove consolitated columns *
      --mode [append|overwrite|add|mean], -e [append|overwrite|add|mean]
                            Consolidation mode for cells with same header and row
                            id. One of: append (old_value;new_value), overwrite,
                            add (numerical addition) or mean (average of numerical
                            values). Default is 'smart_append-;' (append only if
                            value is not already present, use ';' as append
                            delimiter)
    
    Query:
      --columns [COLUMNS [COLUMNS ...]], -k [COLUMNS [COLUMNS ...]]
                            Extract specific columns from data. Default: print all
                            columns
      --query [QUERY [QUERY ...]], -q [QUERY [QUERY ...]]
                            Extract IDs that meet a query (NOTE: will not return
                            IDs with entry in special 'Exclude' column)
      --printHeaders, -H    Prints all column headers and their index
    
    * = can take multiple arguments
    
    Examples:
        pysheet.py -o mystudy.csv -w ID001 Age 38 ID002 Gender M
            create a blank sheet and adds two cell entries to it. Saves it as ./mystudy.csv
        
        pysheet.py -d mystudy.csv -c Items store -q 'Availability Price>0.5'
            consolidate Items across columns whose headers contain keyword 'store'
            then print IDs for Items qith non-blank Availability and price greater than 0.5
        
        pysheet.py -d /path/table.txt -D'\t' -o ./test/mystudy.csv -k 5 1-3 -v
            read a tab-delimited sheet and save columns in the order:
            0 (assumed to be IDs) 5,1,2 and 3, in csv format
    
        pysheet.py -d mystudy.csv mystudy2.csv mystudy3.csv -i 2 2 3 -k
            merge data files specifying the ID column for each & print resulting table to screen
    
        pysheet.py -d mystudy.csv -R 01001 Status -o mystudy.csv
            delete entry for ID '01001' and column 'Status'
    
        touch res.csv; pysheet.py -d res.csv -w iteration_$i Result $val -o res.csv -L
            add an entry to the results file, locking before read/write access
    
        pysheet.py -d table.txt -D '\t' -i -1 -k 2 3 1 -o stdout -O '\t' -n | further_proc
            rearrange columns of tab-delimited file and forward output to stdout
### Pydoc
Generated using:

    import pydoc
    from pysheet import Pysheet
    pydoc.writedoc(Pysheet)

Available at [Pysheet.html](http://htmlpreview.github.io/?https://github.com/isthisthat/Pysheet/blob/master/Pysheet.html)

## Changelog

### v3.14
* Added `mean` as a column consolidation option (thanks @psaffrey-illumina)
* Added support for percentage numeric representations (ending with `%`)
* Added [Travis CI](https://travis-ci.org/isthisthat/Pysheet)

### v3.13
* Added NONE as a keyword for adding cells
* Added ability to specify collapse delimiter in mode

### v3.12
* Added setCell function that correctly updates dictionary keys if updating the ID column

### v3.11
* Added options `--removeMissingRows` and `--removeMissingColumns` to remove rows/columns containing blank values
* Now accepts blank headers and replaces them with V00i where i is the index of the blank header

### v3.10
* Added option `--outFname` to output filename as column. Useful when merging files

### v3.9
* Sorts output in natural order ([natsort](https://pypi.python.org/pypi/natsort))
* Added option `--skipCol` to skip columns from the right of files

### v3.8
* Now detects input delimiter automatically

### v3.7
* Using `OrderedDict` so that if index column is not specified, return rows in input order
* Changed `--hstack` to `--cstack` (columns) and `--vstack` to `--rstack` (rows) for clarity
* Added option to manually specify output headers `--outHeader`

### v3.6
* Added outHeaders option to allow user to replace output headers
* Misc cosmetic fixes

### v3.5
* Fixed a bug when casting a string to a number

### v3.4
* Added `--vstack` and `--hstack` options for cases where you just need to concatenate rows or columns of data
* Shortened the name of some of the command-line parameters
* Misc improvements

### v3.3
* Misc improvements

### v3.2
* Pysheet is now all packaged up and available through `pip` (and `easy_install`) yay!

### v3.1
* Lots of bugfixes
* Now more robust with empty files

### v3.0
* Many speed and stability improvements
* Moved merge options to just be additional arguments of the data options
* Using a unique ID for all auto-IDs
* Fixed bug when merging files with auto-IDs

### v2.2
* Added option to transpose input (`-t`)
* If table is too wide to print in the terminal, give a message instead of an exception
* Cleaned up help a bit and used some of pylint's suggestions

### v2.1
* Added option to read in a spreadsheet that has no headers (`-n`)
* Added option to control what happens when the user attempts to overwrite a cell (see `--mode`)
* Fixed minor bugs

### v2.0
* Added input/output from/to `stdin`/`stdout`
* Indexing column can now be user-specified
* Enabled automatic indexing (in case there is no unique-ID column)
* Added options to deal with headers and comments
* Added tests!
* Made `README` and example
* Lots of refactoring to make code more robust

### v1.0
* Read, write, merge, consolidate and query spreadsheets
* Concurrent access control


## TODO
* Allow indexing by more than one column (e.g. chromosome\_position\_allele)
* Allow creation of SubSheets, i.e. to extract certain rows and columns from a Pysheet
* Integrate version control (perhaps git) for disaster recovery of spreadsheets operated on
* Please [let me know](https://github.com/isthisthat/Pysheet/issues) if you'd like to see more features!


## Help!
I hope you find pysheet useful. If you need more help, please [contact me](https://github.com/isthisthat)! I'd be happy to hear from you.  
Please submit feature requests and bug reports [via github](https://github.com/isthisthat/Pysheet/issues).

