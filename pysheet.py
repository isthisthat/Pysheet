#!/usr/bin/env python

"""
A library to read, write and manipulate delimited text files

Copyright (c) 2014, Stathis Kanterakis
Last Update: April 2014
"""

__version__ = "3.6"
__author__  = "Stathis Kanterakis"
__license__ = "LGPL"

import csv, sys, os, logging, re, traceback
import argparse
from numpy import reshape, floating
from types import IntType
from itertools import izip
import cPickle
from time import sleep
from datetime import datetime
from random import random
from signal import signal, SIGPIPE, SIG_DFL

# don't throw exceptions on closed pipes..
signal(SIGPIPE,SIG_DFL)

# unbuffered stdout
UNBUFF = os.fdopen(sys.stdout.fileno(), 'w', 0)
sys.stdout = UNBUFF

# Should we profile the code?
PROFILE = False
PROFILE_LIMIT = 0.1 # %
PROFILE_DAT = 'profile.dat'
PROFILE_TXT = 'profile.txt'

####################################
########### CLI WRAPPER ############
####################################

def main():
    """parse command line input and call appropriate functions"""

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, \
    description="A library to read and write delimited text files", epilog="""
* = can take multiple arguments

Examples:
    %(prog)s -o mystudy.csv -w ID001 Age 38 ID002 Gender M
        create a blank sheet and adds two cell entries to it. Saves it as ./mystudy.csv
    
    %(prog)s -d mystudy.csv -c Items store -q 'Availability Price>0.5'
        consolidate Items across columns whose headers contain keyword 'store'
        then print IDs for Items qith non-blank Availability and price greater than 0.5
    
    %(prog)s -d /path/table.txt -D'\\t' -o ./test/mystudy.csv -k 5 1-3 -v
        read a tab-delimited sheet and save columns in the order:
        0 (assumed to be IDs) 5,1,2 and 3, in csv format

    %(prog)s -d mystudy.csv mystudy2.csv mystudy3.csv -i 2 2 3 -k
        merge data files specifying the ID column for each & print resulting table to screen

    %(prog)s -d mystudy.csv -R 01001 Status -o mystudy.csv
        delete entry for ID '01001' and column 'Status'

    touch res.csv; %(prog)s -d res.csv -w iteration_$i Result $val -o res.csv -L
        add an entry to the results file, locking before read/write access

    %(prog)s -d table.txt -D '\\t' -i -1 -k 2 3 1 -o stdout -O '\\t' -nh | further_proc
        rearrange columns of tab-delimited file and forward output to stdout
""")
    groupI = parser.add_argument_group('Input')
    groupI.add_argument('--data', '-d', type=readable, nargs='*', metavar="FILE", \
            default=[None], help='Delimited text file with unique IDs in first column '
            '(or use -i) and headers in first row. Or "stdin *"')
    groupI.add_argument('--delim', '-D', metavar='CHAR', nargs='*', default=[','], \
            help='Delimiter of data. Default is comma *')
    groupI.add_argument('--idCol', '-i', type=int, nargs='*', default=[0], \
            metavar='INT', help='Column number (starting from 0) of unique IDs. Or "-1" '
            'to auto-generate. Default is 0 (1st column) *')
    groupI.add_argument('--noHeader', '-n', type=yesNo, nargs='*', default=[False], \
            metavar='Y|N', help='Data file does not contain headers *')
    groupI.add_argument('--skipCol', '-s', type=int, nargs='*', default=[0], \
            metavar='INT', help='Skip this number of rows from top of file *')
    groupI.add_argument('--trans', '-t', type=yesNo, nargs='*', default=[False], \
            metavar='Y|N', help='Read data transposed *')
    groupI.add_argument('--vstack', '-vs', action='store_true', \
            help='Stack input files by rows (sets auto headers)')
    groupI.add_argument('--hstack', '-hs', action='store_true', \
            help='Stack input files by columns (sets auto IDs)')

    groupO = parser.add_argument_group('Output')
    groupO.add_argument('--out', '-o', type=writeable, metavar="FILE", \
            help='Output filename (may include path). Or "stdout" *')
    groupO.add_argument('--outDelim', '-O', metavar='CHAR', \
            help='Delimiter of output file. Default is comma', default=',')
    groupO.add_argument('--outHeader', '-OH', nargs='*', metavar="HEADER", \
            help="Replace output header with this list")
    groupO.add_argument('--outNoHeader', '-N', action='store_true', \
            help="Don't output header row at the top")
    groupO.add_argument('--outTrans', '-T', action='store_true', help='Write output transposed')

    groupRW = parser.add_argument_group('Read/Write')
    groupRW_me = groupRW.add_mutually_exclusive_group()
    groupRW_me.add_argument('--write', '-w', nargs='*', metavar="ID HEADER VALUE", \
            help="Write new cells *")
    groupRW_me.add_argument('--read', '-r', nargs='*', metavar="ID HEADER", \
            help="Print value of cells *")
    groupRW_me.add_argument('--remove', '-R', nargs='*', metavar="ID HEADER", \
            help="Remove cells *")
    groupRW.add_argument('--lockFile', '-L', nargs='?', type=writeable, \
            help="Read/write lock to prevent parallel jobs from overwriting the data. "
            "Use in asynchronous loops. You may specify a filename (default is <out>.lock)", \
                    const=True)

    groupC = parser.add_argument_group('Consolidate')
    groupC.add_argument('--consolidate', '-c', nargs='*', action='append', \
            metavar="HEADER KEYWORD1 KEYWORD2 etc", help='Consolidate columns according to keywords *')
    groupC.add_argument('--clean', '-C', nargs='*', action='append', \
            metavar="HEADER KEYWORD1 KEYWORD2 etc", help="Consolidate and remove consolitated columns *")
    groupC.add_argument('--mode', '-e', nargs='?', \
            choices=['append','overwrite','add','smart_append'], default='smart_append', \
            metavar="append|overwrite|add", help="Consolidation mode for cells with same header "
            "and row id. One of: append (old_value;new_value), overwrite or add "
            "(numerical addition). Default is 'smart_append' (append only if value is "
            "not already present)")

    groupQ = parser.add_argument_group('Query')
    groupQ.add_argument('--columns', '-k', nargs='*', \
            help="Extract specific columns from data. Default: print all columns")
    groupQ.add_argument('--query', '-q', nargs='*', help="Extract IDs that meet a query "
            "(NOTE: will not return IDs with entry in special 'Exclude' column)")
    groupQ.add_argument('--printHeaders', '-H', action='store_true', \
            help="Prints all column headers and their index")

    # this is for testing purposes. will sleep before writing to test locking stuff..
    parser.add_argument('--wait', type=int, help=argparse.SUPPRESS)

    parser.add_argument('catchall', nargs=argparse.REMAINDER, help=argparse.SUPPRESS)

    parser.add_argument('--version', '-V', action='version', version="%(prog)s v" + __version__)
    parser.add_argument('--verbose', '-v', action='count', help='verbosity level')

    # if no arguments given
    if len(sys.argv) < 2:
        parser.print_usage()
        sys.exit(1)
    else:
        args = parser.parse_args()

    # setup the logger. we'll use the process ID for the name
    myPid = str(os.getpid())
    logger = logging.getLogger(myPid)
    if args.verbose == 1:
        logger.setLevel(logging.INFO)
    elif args.verbose > 1:
        logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s', \
            "%Y-%m-%d %H:%M:%S")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # are there leftover parameters?
    if args.catchall:
        logger.warn("!!! Unused parameters: %s" % flatten(args.catchall))

    # check some boolean parameters for zero-length and assign value
    if args.noHeader == []:
        args.noHeader = [True]
    if args.trans == []:
        args.trans = [True]

    # check lists of items for consistency
    numOfSheets = len(args.data)
    if numOfSheets > 0:
        if args.data.count("stdin") > 1:
            logger.critical("!!! You can't have two inputs from stdin")
            sys.exit(1)
        check_this = ["delim", "idCol", "skipCol", "noHeader", "trans"]
        for check_name in check_this:
            assert hasattr(args, check_name)
            check_values = getattr(args, check_name)
            if len(check_values) != numOfSheets:
                if len(check_values) == 0:
                    logger.critical("!!! No %s value given. Please provide up to %d" % \
                            (check_name, numOfSheets))
                    sys.exit(1)
                elif len(check_values) == 1:
                    # make this the same for all data files
                    setattr(args, check_name, check_values * numOfSheets)
                elif len(check_values) < numOfSheets:
                    logger.warn("!!! Too few %s: %d. Using last %s (%s) for %d more data files" % \
                            (check_name, len(check_values), check_name, check_values[-1], \
                            numOfSheets - len(check_values)))
                    setattr(args, check_name, check_values + [check_values[-1]] * \
                            (numOfSheets - len(check_values)))
                else:
                    logger.warn("!!! Too many %s: %d. Required up to: %d. Disregarding the rest" % \
                            (check_name, len(check_values), numOfSheets))

    # save once at the end if needed
    save = False
    if args.out: save = True

    # some IO info
    if args.data:
        if len(args.data) == 1:
            logger.info("+++ Input file: %s" % args.data[0])
        elif len(args.data) <= 10:
            logger.info("+++ Input files (%d):\n%s" % (len(args.data), flatten(args.data, '\n')))
        elif len(args.data) > 10:
            logger.info("+++ Input files (%d):\n%s..." % \
                    (len(args.data), flatten(args.data[:10], '\n')))
    if args.out:
        logger.info("+++ Output file: %s" % args.out)

    # LOCKING
    lock = False
    if args.lockFile:

        # check that we have something to lock
        if args.out:

            # set lockFile and check that we are not overwriting critical stuff..
            if args.lockFile == True:
                args.lockFile = os.path.realpath(args.out + ".lock")
            if os.path.isfile(args.lockFile):
                try:
                    if args.out and os.path.samefile(args.lockFile, args.out):
                        logger.critical("!!! lockFile can't be the same as your output file!")
                        sys.exit(1)
                    if args.data and os.path.samefile(args.lockFile, args.data[0]):
                        logger.critical("!!! lockFile can't be the same as your data file!")
                        sys.exit(1)
                except OSError: # file was deleted in the meantime
                    pass

            args.lockFile = writeable(args.lockFile) # check it is writeable

            lock = True
            logger.info("+++ Lock file: %s" % args.lockFile)
        else:
            logger.warn("!!! Locking makes no sense unless you specify an output...")

    # perform locking ?
    if lock:
        counter = 1.0
        timeoutSec = 180.0
        staleSec = timeoutSec + 2
        # see if there are leftover locks
        if os.path.isfile(args.lockFile):
            try:
                lockTime = datetime.fromtimestamp(os.path.getmtime(args.lockFile))
                curTime = datetime.now()
                if (curTime - lockTime).seconds > staleSec:
                    logger.warn("!!! Removing stale lock: %s" % args.lockFile)
                    os.remove(args.lockFile)
            except OSError: # lock was already removed!
                pass
        # wait for all other instances to finish writing
        while counter < timeoutSec:
            if os.path.isfile(args.lockFile):
                counter += random()
                sleep(counter)
            else: # attempt a lock
                open(args.lockFile,'w').write(myPid)
                sleep(random())
                try:
                    thisPid = open(args.lockFile,'rU').read()
                    if thisPid == myPid: # we've successfully acquired a lock!
                        break
                except IOError: # somebody removed the lock externally...
                    continue
        if counter > 1:
            logger.debug(">>> Process slept for %d sec waiting for lock..." % counter)
        # we've exceeded the timeoutSec and a process is still writing. now what?
        if counter >= timeoutSec:
            args.out += "." + myPid
            logger.critical("""
!!! LOCK TIMEOUT LIMIT EXCEEDED (%(lt)d seconds)
!!! Changes (if any) to: %(ds)s
!!! will be saved to: %(os)s
!!! To merge changes back, use:
!!! %(prog)s %(ds)s %(os)s -o %(ds)s""" % \
        {"lt":timeoutSec, "ds":args.data[0], "os":args.out, "prog":sys.argv[0]})
            lock = False
        else:
            logger.debug(">>> Grabbing lock...")

    # check if output exists
    if args.out and args.out != 'stdout' and os.path.isfile(args.out) and not args.out in args.data:
        logger.warn("!!! Output file already exists and will be overwritten: %s" % args.out)
    if args.out and not args.data:
        logger.warn(">>> Creating a blank sheet in: %s" % args.out)

    try:
        # now read the file
        mycsv = Pysheet(args.data[0], delimiter=args.delim[0], idColumn=args.idCol[0], \
                skip=args.skipCol[0], noHeader=args.noHeader[0], vstack=args.vstack, \
                hstack=args.hstack, trans=args.trans[0])

        # merge
        if numOfSheets > 1:
            for m in range(1, numOfSheets):
                myothercsv = Pysheet(args.data[m], delimiter=args.delim[m], \
                        idColumn=args.idCol[m], skip=args.skipCol[m], \
                        noHeader=args.noHeader[m], vstack=args.vstack, \
                        hstack=args.hstack, trans=args.trans[m])
                mycsv += myothercsv # __add__
                mycsv.contract(mode=args.mode) # merge same columns

        # remove cells
        if args.remove:
            try:
                cells = reshape(args.remove,(len(args.remove)/2,2))
                printed = 0
                for i in range(len(cells)):
                    if cells[i][1].lower() == "none":
                        ret = mycsv.removeCell(key=cells[i][0])
                        #if ret: save = True
                        if isList(ret):
                            ret = '|'.join(ret)
                    else:
                        ret = mycsv.removeCell(key=cells[i][0],header=cells[i][1])
                        #if ret: save = True
                    if ret:
                        sys.stdout.write(str(ret))
                        printed += 1
                logger.info("=== Deleted %d cell%s.." % (printed, '' if printed==1 else 's'))
            except ValueError:
                logger.critical("!!! Cell entries must be of the form 'ID header': %s" % \
                        flatten(args.remove))
                sys.exit(1)


        # add cells
        if args.write:
            try:
                cells = reshape(args.write,(len(args.write)/3,3))
                #save = True
                for i in range(len(cells)):
                    if cells[i][1].lower() == "none":
                        mycsv.addCell(cells[i][0], mode=args.mode)
                    else:
                        mycsv.addCell(cells[i][0],cells[i][1],cells[i][2], mode=args.mode)
                logger.info("=== Added %d cell%s.." % (len(cells), '' if len(cells)==1 else 's'))
            except ValueError:
                logger.critical("!!! Cell entries must be of the form 'ID header value': %s" % \
                        flatten(args.write))
                sys.exit(1)


        # consolidate
        if args.clean:
            for c in args.clean:
                logger.info(">>> Consolidating (clean): %s" % flatten(c) if c else "same headers")
            mycsv.consolidate(args.clean, cleanUp=True, mode=args.mode)
        if args.consolidate:
            for c in args.consolidate:
                logger.info(">>> Consolidating: %s" % flatten(c) if c else "same headers")
            mycsv.consolidate(args.consolidate, mode=args.mode)


        # query
        # by columns
        if args.columns != None: # we still need to handle []
            if not args.columns == []:
                # if we got some column spec, extract columns (else print all)
                cols = Pysheet()
                cols.obj_id = "output" + cols.obj_id
                cols.load(mycsv.getColumns(args.columns, blanks=True, exclude=False))
                mycsv = cols # make this the current spreadsheet
            if not args.out and not args.query and not args.read and not args.printHeaders:
                sys.stdout.write(str(mycsv))
        # print headers
        if args.printHeaders:
            for hi in range(len(mycsv.getHeaders())):
                sys.stdout.write("%d %s\n" % (hi, mycsv.getHeaders()[hi]))
        # by rows
        if args.query:
            retList = transpose(mycsv.getColumns(args.query))[0][1:]
            # first column is always the IDs; 1: skips the header row (now column)
            retList.sort()
            logger.info("=== Query '%s' returned %d ID%s.." % (flatten(args.query), \
                    len(retList), '' if len(retList)==1 else 's'))
            for item in retList:
                sys.stdout.write(item+"\n")
        # by cells
        if args.read:
            try:
                cells = reshape(args.read,(len(args.read)/2,2))
                printed = 0
                for i in range(len(cells)):
                    if cells[i][1].lower() == "none":
                        ret = mycsv.grab(key=cells[i][0])
                        if isList(ret):
                            ret = '|'.join(ret)
                    else:
                        ret = mycsv.grab(key=cells[i][0],header=cells[i][1])
                    if ret:
                        sys.stdout.write(str(ret))
                        printed += 1
                logger.info("=== Printed %d cell%s.." % (printed, '' if printed==1 else 's'))
            except ValueError:
                logger.critical("!!! Cell entries must be of the form 'ID header': %s" % \
                        flatten(args.read))
                sys.exit(1)

        # now save
        if save:
            if args.wait:
                logger.debug(">>> Sleeping %d sec" % args.wait)
                sleep(args.wait)
            mycsv.save(args.out, args.outDelim, not args.outNoHeader, args.outHeader, args.outTrans)
            logger.info("=== Saved as: %s" % args.out)

    # catch all exception thrown by Pysheet objects
    except PysheetException as e:
        print "!!! Pysheet Error: %s" % e.message
        sys.exit(3)
    except KeyboardInterrupt:
        print "!!! Interrupted"
        sys.exit(1)

    if lock:
        logger.debug(">>> Releasing lock...")
        os.remove(args.lockFile)



####################################
######## CLASS STARTS HERE #########
####################################

class Pysheet:
    """Pysheet - A library to read, write and manipulate spreadsheets"""
    # globals
    HEADERS_ID      = "__header__" # denotes the row which contains the header labels
    EXCLUDE_HEADER  = "__exclude__" # special column header used to flag a row for exclusion
    AUTO_ID_HEADER  = "__AutoID__" # header used for auto ids when negative idColumn specified
    MIN_LINE_LEN    = 2             # minimum length of input line array to treat as valid
    COMMENT_CHAR    = '#'           # lines starting with this character will be excluded
    APPEND_CHAR     = ';'           # character used to merge strings in consolidated columns
    BLANK_VALUE     = ''            # default value used for empty cells
    FLAG_VALUE      = '__flag__'    # flags a row for filtering
    MAX_PRINT_WIDTH = 200           # the max width of a printout on the command line

    # parameters
    filename  = None # the input/output path and name
    delimiter = None # the sheet delimiter
    rows      = None # the dictionary that maps an ID to its row
    idColumn  = 0    # the column index that contains the IDs
    obj_id    = None  # an id to distinguish between objects

    def __init__(self, filename=None, delimiter=',', iterable=None, idColumn=None, skip=0, \
            noHeader=False, vstack=False, hstack=False, trans=False):
        """initializes the object and reads in a sheet from a file or an iterable.
        Optionally specify the column number that contains the unique IDs (starting from 0)"""
        # set IDs
        if not self.obj_id:
            self.obj_id = "_" + randomId() #str(id(self))
            #self.HEADERS_ID = self.HEADERS_ID + self.obj_id
            #self.AUTO_ID_HEADER = self.AUTO_ID_HEADER + self.obj_id
            #self.FLAG_VALUE = self.FLAG_VALUE + self.obj_id
        else:
            sys.stderr.write("!!! Re-initialising %s\n" % self.obj_id)
        # set filename
        self.filename = filename
        # set ID column
        if idColumn != None:
            try:
                self.idColumn = int(idColumn)
            except ValueError as e:
                self.idColumn = 0
        # set delimiter
        if delimiter == r'\t':
            self.delimiter = "\t"
        elif delimiter == r'\s':
            self.delimiter = "\s"
        else:
            self.delimiter = delimiter
        # initialize dictionary
        #self.rows = {}
        # and call the appropriate loader
        if filename and (os.path.exists(filename) or filename == 'stdin'):
            self.loadFile(self.filename, self.idColumn, skip, noHeader, vstack, hstack, trans)
        elif iterable:
            self.load(iterable, self.idColumn, skip, noHeader, vstack, hstack, trans)
        else:
            self.clear()

    def loadFile(self, filename, idColumn=None, skip=0, noHeader=False, vstack=False, \
            hstack=False, trans=False):
        """loads the sheet into a dictionary where the IDs in the first column are
        mapped to their rows. Optionally specify the column number that contains
        the unique IDs (starting from 0)"""
        try:
            if filename == 'stdin':
                reader = csv.reader(sys.stdin, delimiter=self.delimiter)
            else:
                reader = csv.reader(open(filename, "rUb"), delimiter=self.delimiter)
        except csv.Error as e:
            raise PysheetException(e, filename)
        self.filename = filename
        if idColumn != None:
            try:
                self.idColumn = int(idColumn)
            except ValueError as e:
                self.idColumn = 0
        self.load(reader, self.idColumn, skip, noHeader, vstack, hstack, trans)

    def load(self, iterable, idColumn=None, skip=0, noHeader=False, vstack=False, \
            hstack=False, trans=False):
        """creates a Pysheet object from an iterable.
        Optionally specify the column number that contains the unique IDs (starting from 0)"""
        name = os.path.basename(self.filename) if self.filename else self.obj_id
        # if empty set headers and return
        if not iterable:
            sys.stderr.write("!!! Null iterbale. Clearing object %s\n" % name)
            self.clear()
            return
        # try iterating it
        try:
            iterator = iter(iterable)
        except TypeError:
            raise PysheetException("%s is not iterable!" % iterable)

        # if vstack (rows) then we need auto headers internally
        if vstack:
            noHeader = True

        # if hstack (cols) then we need auto ids internally
        if hstack:
            idColumn = -1

        # set id column
        row = 0
        head_len = -1
        if idColumn != None:
            try:
                self.idColumn = int(idColumn)
                if self.idColumn < 0: # auto-generate ids!
                    self.idColumn = -1
                    self.MIN_LINE_LEN -= 1 # accept smaller lines
            except ValueError:
                self.idColumn = 0

        # do the skipping now
        skip_counter = 0
        while skip_counter < skip:
            discard = iterator.next()
            skip_counter += 1

        # do transpose
        if trans:
            cols = []
            max_line_len = 0
            try:
                while True: # need to go through file to remove comments & make sure it's square
                    line = iterator.next()
                    line_len = len(line)
                    # skip blanks and comments
                    if line_len < max(self.MIN_LINE_LEN, 1) or \
                            str(line[0]).startswith(self.COMMENT_CHAR):
                        continue
                    if line_len < max_line_len:
                        line = line + [self.BLANK_VALUE] * (max_line_len - line_len)
                    elif line_len > max_line_len: # we need to adjust all previous lines..
                        for i in range(len(cols)):
                            cols[i] += [self.BLANK_VALUE] * (line_len - max_line_len)
                        max_line_len = line_len
                    cols.append(line)
            except StopIteration:
                pass
            iterator = iter(zip(*cols))
        
        # clear the object
        self.clear()

        # start reading
        try:
            line = None
            while True:
                line = iterator.next()
                line_len = len(line)
                # skip blank, short lines and comments
                if line_len < max(self.MIN_LINE_LEN, 1) or str(line[0]).startswith(self.COMMENT_CHAR):
                    continue
                # header row
                if row == 0:
                    head_len = line_len
                    if self.idColumn >= head_len:
                        raise PysheetException("Invalid id column. Maximum is %d (starting from 0)" % \
                                (head_len-1))
                    # load headers
                    if noHeader:
                        if vstack:
                            self.rows[self.HEADERS_ID] = ["C%03d" % (col+1) \
                                    for col in range(head_len)]
                        else: # else add the object's unique id
                            self.rows[self.HEADERS_ID] = ["C%03d%s" % (col+1, self.obj_id) \
                                    for col in range(head_len)]
                        row+=1 # so that this line gets added below
                    else:
                        self.rows[self.HEADERS_ID] = [str(value).strip() for value in line]
                    # did they request auto ids?
                    if self.idColumn == -1:
                        self.rows[self.HEADERS_ID].append(self.AUTO_ID_HEADER)
                # rest or rows
                if row > 0:
                    thisline = []
                    for value in line[:head_len]:
                        thisline.append(value)
                    if line_len > head_len: # we have a problem
                        sys.stderr.write(("!!! Line %d is longer than your header line (%d vs %d) and "
                        "will be truncated!! Please make sure every column has a header\n") % \
                                (row+1, line_len, head_len))
                        line_len = head_len
                    elif line_len < head_len:
                        while line_len < head_len:
                            thisline.append(self.BLANK_VALUE)
                            line_len += 1
                    if self.idColumn == -1: # auto-generate ids!
                        if hstack:
                            thisline.append("R%05d" % row)
                        else:
                            thisline.append("R%05d%s" % (row, self.obj_id))
                        line_len += 1
                    self.rows[clean(sanitize(thisline[self.idColumn]))] = thisline
                # move to next row
                row += 1

        except StopIteration:
            # print some status messages
            discarded = row - self.height()
            if head_len == -1 and line != None:
                sys.stderr.write("+++ %s: empty (wrong delimiter or too few columns)\n" % name)
            elif head_len == -1:
                sys.stderr.write("+++ %s: empty\n" % name)
            elif discarded > 0:
                sys.stderr.write("+++ %s: %d rows (%d discarded: duplicate ids), %d columns\n" % \
                    (name, self.height(), discarded, head_len))
            else:
                sys.stderr.write("+++ %s: %d rows, %d columns\n" % (name, self.height(), head_len))
            # set proper idColumn if auto
            if self.idColumn == -1:
                self.idColumn = head_len
        except Exception as e:
            printStackTrace()
            raise PysheetException(e.message)

    def clear(self):
        """clears the object"""
        self.rows={self.HEADERS_ID:["ID"]}

    def __iter__(self):
        """returns an iterator over the ID:row items in the csv"""
        return self.rows.iteritems()

    def __len__(self):
        """returns the length of the header row in the dictionary"""
        return len(self.getHeaders())

    def height(self):
        """returns the number of rows in the dictionary"""
        return len(self.rows)

    def pop(self, x, default=None):
        """pops an item out of the dictionary"""
        return self.rows.pop(clean(x),default)

    def keys(self, headers=True, exclude=True, lockedRows=True):
        """returns a list of the keys in the dictionary.
        headers=True adds columnheaders to the list returned
        exclude=True skips rows with a non-blank __exclude__ column
        lockedRows=True also returns rows whose ID starts with '__'"""
        return [self[x][self.idColumn] for x in self.rows.keys() if \
                (headers or x != self.HEADERS_ID) and (lockedRows or not x.startswith('__')) and \
                (not exclude or not self.excluded(x))]

    def getIds(self):
        """returns all keys of the dictionary, except the header
        key and any IDs starting with '__'"""
        return self.keys(headers=False, exclude=True, lockedRows=False)

    def __getitem__(self, key, default=None):
        """gets the row of an ID from the dictionary"""
        return self.rows.get(clean(sanitize(key)), default)
    def get(self, key, default=None):
        """gets the row of an ID from the dictionary"""
        return self.__getitem__(key, default)

    def __setitem__(self, key, row):
        """changes the row of an ID in the dictionary"""
        self.rows[clean(key)] = row
    def set(self, key, row):
        """changes the row of an ID in the dictionary"""
        self[key] = row

    def __delitem__(self, key):
        """deletes a row from the dictionary"""
        del self.rows[clean(key)]

    def headerIndex(self, header):
        """returns the index of a header in the dictionary"""
        # if a column number, return the index
        if header != None:
            header = clean(str(header)).replace('__','')
            headers_lower = [l.lower().replace('__','') for l in self.getHeaders()]
            if header in headers_lower:
                return headers_lower.index(header)
            elif str(header).isdigit():
                header = int(header)
                if header >= 0 and header < len(self):
                    return header
        return -1 # all other cases return -1 to indicate error

    def getHeaders(self): #, lockedHeaders=True):
        """returns the headers of the columns in the dictionary.
        If lockedHeaders=False, skips the headers starting with '__'"""
        #if not lockedHeaders:
        #    return [l for l in self[self.HEADERS_ID] if not l.startswith('__')]
        return self[self.HEADERS_ID]

    def containsColumn(self, header):
        """True if header exists in the csv header list"""
        return self.headerIndex(header) > -1

    def addCell(self, key, header=None, value=None, mode='overwrite'):
        """adds a single cell in the dictionary. mode can be 'smart_append'
        (adds a value if not already present), 'append', 'overwrite' and
        'add' (performs plus operation if values are numeric)"""

        cleankey = clean(sanitize(key))
        if self[cleankey] == None:
            self[cleankey] = [key.strip()] + [self.BLANK_VALUE] * (len(self)-1)
        if header != None:
            header = header.strip()
            if self.headerIndex(header) == -1: # add the header
                if header.startswith('__'):
                    self.insertColumn(header)
                else:
                    self.getHeaders().append(header)
                    self.expand()
            if value == None:
                value = self.BLANK_VALUE
            # add the value using the correct mode
            self[cleankey][self.headerIndex(header)] = \
                    self.mergedValue(self[cleankey][self.headerIndex(header)], value, mode=mode)

    def removeCell(self, key, header=None):
        """deletes a cell or a row from the dictionary"""
        ret = None
        cleankey = clean(sanitize(key))
        if self[cleankey] != None:
            if header != None:
                header = header.strip()
                if self.headerIndex(header) != -1: # header exists
                    ret = self[cleankey][self.headerIndex(header)]
                    self[cleankey][self.headerIndex(header)] = self.BLANK_VALUE
            else:
                ret = self[cleankey]
                del self[cleankey]
        return ret

    def insertColumn(self, header, index=None):
        """inserts a blank column in the dictionary at index (number)"""
        header = str(header)
        if not header.lower().replace('__','') in [l.lower().replace('__','') \
                for l in self.getHeaders()]:
            if index == None:
                index = 1
                while index < len(self) and self.getHeaders()[index].startswith('__'):
                    index+=1
            else:
                assert type(index) is IntType, "index is not an integer: %r" % index
                assert index >= 0 and index <= len(self), \
                        "index is not in a valid range [%d-%d]: %d" % (0, len(self), index)

            for k in self.rows.keys():
                if k == self.HEADERS_ID:
                    if not header.strip().startswith('__'):
                        header = '__'+header.strip()
                    self[k].insert(index,header)
                else:
                    self[k].insert(index,self.BLANK_VALUE)
            # did we insert before the idColumn?
            if index <= self.idColumn:
                self.idColumn += 1

    def grab(self, key=None, header=None, level=None):
        """grabs a cell (key + header), a whole row (just key), or all keys that
        correspond to 'level' (header + level) from the dictionary. level='ALL' is valid"""
        if key != None:
            cleankey = clean(key)
            if self[cleankey] == None:
                return None
            if header != None:
                header = header.strip()
                if self.headerIndex(header) == -1:
                    return None
                return self[cleankey][self.headerIndex(header)] # return the item
            return self[cleankey] # return the whole row
        elif level != None and header != None:
            if self.headerIndex(header) == -1:
                return None
            level = tryNumber(str(level).lower())
            thiscol = self.produceColumn(header)
            ret = []
            for i in range(len(thiscol[0])):
                if (level == 'all' and not self.isBlank(str(thiscol[1][i])) \
                        and not str(thiscol[0][i]).startswith('__')) \
                        or tryNumber(str(thiscol[1][i]).lower()) == level:
                    ret.append(thiscol[0][i])
            return ret
        else:
            raise PysheetException("grab either requires a key, or a header AND a level!")

    def __add__(self, other, mergeHeaders=False):
        """merges two Pysheets together"""
        # see if it is a pysheet
        try:
            other.HEADERS_ID
        except:
            try:
                other = Pysheet(iterable=other) # if we have an iterable, make a Pysheet on the fly
                sys.stderr.write("+++ Cast pysheet %s\n" % other.obj_id)
            except Exception:
                printStackTrace()
                raise PysheetException("I don't know how to add this. Please use a Pysheet")
        # remember old length
        oldlen = len(self)
        # merge the existing IDs
        merged = False
        for i in self.rows.keys():
            # check for headers row
            if i == self.HEADERS_ID:
                item = other.pop(other.HEADERS_ID)
            else: # pops item i or None if not found
                item = other.pop(i,None)
            if item != None and isList(item) and len(item) > 0:
                self[i] += item
                merged = True # merged at least one thing
        # loop through the rest of the IDs, blank-padding to the left
        for i in other.rows.keys():
            self[i] = [self.BLANK_VALUE] * oldlen + other[i]
            merged = True
        # make it sqare again
        self.expand()
        # make ID column header names the same so they will be merged in the next step!
        if merged:
            self.getHeaders()[oldlen + other.idColumn] = self.getHeaders()[self.idColumn]
        # merge common headers?
        if mergeHeaders:
            self.contract()
        return self

    def excluded(self, key):
        """returns True if an item's exclusion header is non-blank"""
        if key == self.HEADERS_ID:
            return False
        return not self.isBlank(self.grab(key=key, header=self.EXCLUDE_HEADER))

    def parseColumns(self, cols):
        """parses the input column specification. For example expands ["5","1-3","Age>13"]
        to [[5,1,2,3,9],['','','','','>'],['','','','',13]]
        Returns list of corresponding [[header index, ...], [operator, ...], [argument, ...]]"""
        ret = [[],[],[]]
        if cols:
            if not isList(cols):
                cols=[cols]
            if 'ALL' in cols: # use all columns
                rng = range(self.idColumn) + range(self.idColumn+1, len(self))
                ret = [rng, ['']*len(rng), ['']*len(rng)]
                cols.remove("ALL")
            for i in range(len(cols)):
                added = False # flag to track if we were able to parse this column or not
                c = cols[i]
                hi = self.headerIndex(c)
                if hi >= 0:
                    ret[0].append(hi) # add column as is
                    ret[1].append('')
                    ret[2].append('')
                    added = True
                else:
                    # try ranges
                    try:
                        frm,to = c.split("-",1)
                        if frm.isdigit():
                            frm = int(frm)
                            if to.isdigit(): # it is of the form e.g. '5-10'
                                to = int(to)
                                if to >= frm and frm < len(self) and to < len(self):
                                    rng = range(frm, to+1)
                                    ret[0].extend(rng)
                                    ret[1].extend(['']*len(rng))
                                    ret[2].extend(['']*len(rng))
                                    added = True
                            elif not to: # it is of the form e.g. '5-'
                                # which means from column 5 till the last column
                                rng = range(frm, len(self))
                                ret[0].extend(rng)
                                ret[1].extend(['']*len(rng))
                                ret[2].extend(['']*len(rng))
                                added = True
                    except (TypeError, ValueError):
                        pass
                    if not added:
                        # try operators
                        arg = ''
                        op    = ''
                        try:
                            if '<' in c:
                                op+='<'
                            if '>' in c:
                                op+='>'
                            if '=' in c:
                                op+='='
                            if '!' in c:
                                op+='!'
                            if '~' in c:
                                op+='~'
                            if '+' in c:
                                op+='+'
                        except TypeError:
                            pass
                        if len(op) == 1: # if there is an operator,
                            # capture the column (eg. age) and the argument (eg. 10)
                            col,arg = c.split(op,1)
                            if arg:
                                hi = self.headerIndex(col)
                                if hi >= 0: #and ind != self.idColumn:
                                    # append [column index, operator (if any), argument (if any)]
                                    ret[0].append(hi)
                                    ret[1].append(op)
                                    ret[2].append(tryNumber(arg))
                                    added = True
                if not added:
                    raise PysheetException("Column '%s' cannot be parsed!" % c)
        return ret

    def produceColumn(self, col=0, blanks=True, exclude=True):
        """extracts a column and corresponding IDs from the dictionary (no column headers)
        returns a 2D list where [0] is a list of IDs and [1] is the requested column
        Input a list of columns to make a hybrid column eg. col=[3,6,5]
        blanks=False will remove rows that contain *any* blank column entries whatsoever
        exclude=True will skip rows that have a value in the __exclude__ column
        Default is return a list of a list of IDs"""
        # first expand the column specification
        cols = self.parseColumns(col)

        # check if we have columns to join
        if not cols[0]:
            return [self.keys(headers=False)] # return all IDs

        # case we have some columns to join
        ret = []
        for i in self.rows.keys():
            hybrid = []
            if i == self.HEADERS_ID or (exclude and self.excluded(i)):
                continue
            for j in range(len(cols[0])): # add in the columns that we want
                if not cols[1][j] or not cols[2][j] or \
                (cols[1][j] == '<' and (tryNumber(self[i][cols[0][j]]) < cols[2][j])) or \
                (cols[1][j] == '>' and (tryNumber(self[i][cols[0][j]]) > cols[2][j])) or \
                (cols[1][j] == '=' and (tryNumber(self[i][cols[0][j]]) == cols[2][j])) or \
                (cols[1][j] == '~' and (str(cols[2][j]) in str(self[i][cols[0][j]]))) or \
                (cols[1][j] == '!' and (tryNumber(self[i][cols[0][j]]) != cols[2][j])):
                    hybrid.append(self[i][cols[0][j]])
                else:
                    hybrid.append(self.BLANK_VALUE)
            if not blanks and self.BLANK_VALUE in hybrid:
                continue
            ret.append([self[i][self.idColumn], "_".join([str(x) for x in hybrid \
                    if not self.isBlank(x)])]) # append the ID and a join of the requested columns
        return transpose(ret) # transpose so that [0] are IDs and [1] is group assignment

    def getColumns(self, cols=None, blanks=False, exclude=True):
        """extracts columns and corresponding IDs from the dictionary (with column headers)
        returns requested columns, row-by-row. Supports operators like cols='age>20'.
        To get multiple columns e.g. set cols=[3,6,5]
        Valid column operators: > (greater than), < (less than), = (equals), ! (does not equal),
        ~ (contains), =UNIQUE (will only keep one of each duplicate in the column)
        blanks=False will remove rows that contain *any* blank column entries whatsoever
        exclude=True will skip rows that have a value in the __exclude__ column
        Default is return all columns"""
        # first expand the column specification
        extrct = self.parseColumns(cols)

        # check if we have columns to return
        if not extrct[0]: # return all columns
            all_header_ind = range(self.idColumn) + range(self.idColumn+1, len(self))
            extrct = [all_header_ind, [self.BLANK_VALUE]*len(all_header_ind), \
                    [self.BLANK_VALUE]*len(all_header_ind)]

        # case we have some columns to return
        ret = []
        for i in self.rows.keys():
            if exclude and self.excluded(i):
                continue
            add = [] # initialize the row to be appended
            for j in range(len(extrct[0])):
                # add in the columns that we want (loops through column idices)
                if i == self.HEADERS_ID: # add operator to the name of the header!
                    if not extrct[2][j]:
                        add.append(self[i][extrct[0][j]])
                    else:
                        if extrct[1][j] == '<':
                            add.append(self[i][extrct[0][j]] + '<' + str(extrct[2][j]))
                        if extrct[1][j] == '>':
                            add.append(self[i][extrct[0][j]] + '>' + str(extrct[2][j]))
                        if extrct[1][j] == '=':
                            add.append(self[i][extrct[0][j]] + '=' + str(extrct[2][j]))
                        if extrct[1][j] == '!':
                            add.append(self[i][extrct[0][j]] + '!' + str(extrct[2][j]))
                        if extrct[1][j] == '~':
                            add.append(self[i][extrct[0][j]] + '~' + str(extrct[2][j]))
                        if extrct[1][j] == '+':
                            add.append(self[i][extrct[0][j]] + '+' + str(extrct[2][j]))
                else: # if we have an operator and argument, check if we satify them
                    if not extrct[1][j] or not extrct[2][j] or \
                    (extrct[1][j] == '<' and (tryNumber(self[i][extrct[0][j]]) < extrct[2][j])) or \
                    (extrct[1][j] == '>' and (tryNumber(self[i][extrct[0][j]]) > extrct[2][j])) or \
                    (extrct[1][j] == '=' and (tryNumber(self[i][extrct[0][j]]) == extrct[2][j])) or \
                    (extrct[1][j] == '~' and (str(extrct[2][j]) in str(self[i][extrct[0][j]]))) or \
                    (extrct[1][j] == '!' and (tryNumber(self[i][extrct[0][j]]) != extrct[2][j])) or \
                    (extrct[2][j] == 'UNIQUE' and (not ret or (ret and self[i][extrct[0][j]] \
                    not in transpose(ret)[j+1]))):
                        add.append(self[i][extrct[0][j]]) # add this value to the new row
                    elif extrct[1][j] == '+':
                        try:
                            add.append(tryNumber(self[i][extrct[0][j]]) + \
                                    tryNumber(extrct[2][j])) # perform addition
                        except TypeError:
                            add.append(str(self[i][extrct[0][j]])+str(extrct[2][j]))
                    else:
                        add.append(self.FLAG_VALUE) # add blank to the new row
            if i == self.HEADERS_ID: # prepend
                ret = [[self[i][self.idColumn]] + [x.replace('__','') for x in add]] + ret
            elif self.FLAG_VALUE in add:
                pass # skip this row
            elif blanks or not self.BLANK_VALUE in add: # if all values there, append to return
                ret.append([self[i][self.idColumn]] + add)
        return ret

    def expand(self):
        """blank-pads to make all rows as long as the headers"""
        headlen = len(self)
        for i in self.rows.keys():
            thislen = len(self[i])
            assert thislen <= headlen, ("Error in row %s. Greater than length "
            "of headers row (%d): %d") % (i, headlen, len(self[i]))
            if thislen < headlen:
                self[i] += [self.BLANK_VALUE] * (headlen - thislen)

    def contract(self, mode='overwrite'):
        """concatenates columns that have the same header. mode can be:
        'append', 'overwrite', 'smart_append' (consolidates values if not already present)
        or 'add' (performs plus operation for numeric values)"""

        deleteme = []
        for i in range(0,len(self)-1):
            for j in range(i+1,len(self)):
                if self.getHeaders()[i].lower().replace('__','') == \
                        self.getHeaders()[j].lower().replace('__',''): # caught the same header!
                    deleteme.append(j)
                    # copy over new values
                    for k in self.rows.keys():
                        if k != self.HEADERS_ID: # skip headers
                            if i == self.idColumn: # if merging IDs also use overwrite
                                self[k][i] = self.mergedValue(self[k][i], self[k][j], mode='overwrite')
                                self.rename(self[k][i], key=k) # re-index just in case
                            else:
                                self[k][i] = self.mergedValue(self[k][i], self[k][j], mode=mode)
        # now delete duplicate columns
        if deleteme:
            self.removeColumns(deleteme)

    def zeroFill(self, zero=0):
        """fills blank cells with zero"""
        for k in self.rows.keys():
            for h in range(len(self[k])):
                if self[k][h] in [None, [], 0, '', self.BLANK_VALUE]:
                    self[k][h]=zero

    def removeColumns(self, cols):
        """removes columns from the dictionary by index, starting from 0 (not by header name)"""
        if cols:
            if not isList(cols):
                cols = [cols]
            else:
                cols.sort()
                cols.reverse()
                cols_unique = unique(cols)
                if len(cols) != len(cols_unique):
                    sys.stderr.write("!!! Non-unique headers detected! Please make sure "
                    "that all your headers are unique and that there are no blank headers\n")
                    cols = cols_unique
            # check columns to be removed
            for c in cols:
                assert type(c) is IntType, "column is not an integer: %r" % c
                if c == self.idColumn:
                    raise PysheetException("Cannot remove the ID Column!")
                assert c >= 0 and c < len(self), "column is not in a valid range [%d-%d]: %d" % \
                        (0, len(self)-1, c)
                if c < self.idColumn:
                    # if we are removing a column which is before our IDs,
                    # then we need to update the idColumn
                    self.idColumn -= 1
            # now remove
            for k in self.rows.keys():
                for c in cols:
                    del self[k][c]

    def rename(self, newName, header=None, key=None):
        """renames a column header, or a row key (not both)"""
        if header:
            hi = self.headerIndex(header)
            if hi >= 0:
                self.getHeaders()[hi] = str(newName).strip()
            else:
                raise PysheetException("Cannot rename. No such header: %s" % header)
        elif key and key != self.HEADERS_ID:
            cleanKey = clean(sanitize(key))
            cleanNewKey = clean(sanitize(newName))
            row = self[cleanKey]
            if row != None:
                if cleanKey != cleanNewKey: # don't rename if the keys are the same
                    del self[cleanKey]
                    row[self.idColumn] = newName
                    self[cleanNewKey] = row
            else:
                raise PysheetException("Cannot rename. No such key: %s" % key)

    def consolidate(self, consolidations, cleanUp=False, mode='smart_append'):
        """consolidates columns according to keywords. consolidations is a 2D list of
        [[header, keyword, keyword, ...], ...]
        Use cleanUp=True to delete the consolidated columns after consolidation
        mode is one of: 'smart_append' (appends new value if not already present),
        'append', 'overwrite' and 'add' (adds up values if numeric)"""

        # contract first to deal with same name headers!
        self.contract(mode=mode)

        # check what the user gave us
        if not consolidations or len(consolidations) == 0 or len(consolidations[0]) == 0:
            return
        if not isList(consolidations):
            consolidations = [consolidations]
        if not isList(consolidations[0]):
            consolidations = [consolidations]

        # now insert the new headers
        consolidationHeaders = izip(*consolidations).next() # get the new headers
        for header in consolidationHeaders:
            self.insertColumn(header)

        # now consolidate
        deleteme = []
        for i in range(len(self)):
            try:
                for h in range(len(consolidationHeaders)):
                    header = consolidationHeaders[h]
                    header_index = self.headerIndex(header)
                    keywords = []
                    if len(consolidations[h]) == 1:
                        # no keywords given! use the header itself as keyword
                        keywords = [k.lower() for k in consolidations[h]]
                    else: # keywords given, skip the header
                        keywords = [k.lower() for k in consolidations[h][1:]]
                    for keyword in keywords:
                        if header_index != i and not self.getHeaders()[i].startswith('__') and \
                                keyword in self.getHeaders()[i].lower(): # caught a similar header!
                            deleteme.append(i)
                            # copy over new values
                            for k in self.getIds():
                                self[k][header_index] = self.mergedValue(self[k][header_index], \
                                        self[k][i], mode=mode)
                            # skip to next column
                            raise StopIteration
            except StopIteration:
                pass

        # now delete copied columns?
        if cleanUp:
            self.removeColumns(deleteme)

    def mergedValue(self, cellA, cellB, mode='smart_append'):
        """returns the merged value of two cells, according to mode:
        'smart_append' (appends new value if not already present), 'append',
        'overwrite' and 'add' (adds up values if numeric)"""
        # check more
        mode = mode.lower()
        if mode not in ['smart_append','append','overwrite','add']:
            raise PysheetException("Merge mode '%s' is invalid!" % mode)

        if self.isBlank(cellA): # clean copy
            return cellB
        else: # merge
            if mode == 'smart_append':
                if not self.isBlank(cellB) and not str(cellB) in \
                        str(cellA).split(self.APPEND_CHAR): # if not already in there
                    return "%s%s%s" % (cellA, self.APPEND_CHAR, cellB)
            elif mode == 'append':
                if not self.isBlank(cellB): # just append
                    return "%s%s%s" % (cellA, self.APPEND_CHAR, cellB)
            elif mode == 'overwrite': # just copy on top
                if not self.isBlank(cellB):
                    return cellB
            elif mode == 'add': # try to do numeric addition
                if not self.isBlank(cellB):
                    if not self.isBlank(cellA):
                        return tryNumber(cellA) + tryNumber(cellB)
                    else:
                        return tryNumber(cellB)
        return cellA # default is the existing value remains

    def levels(self, column, hasHeader=False):
        """returns a tuple containing (the discreet items or 'levels', is a numeric list?,
        the number of levels)"""
        if not isList(column):
            qcolumn = self.produceColumn(column)
            if not qcolumn or len(qcolumn) == 1:
                # our object is blank or this header does not exist!
                return([],False,0)
            else:
                qcolumn=qcolumn[1]
            hasHeader=False
        else:
            qcolumn = column
        offset = 1 if hasHeader else 0
        levs = unique(tryNumber(qcolumn[offset:]), blanks=False)
        return (levs, isNumber(levs), len(levs))

    def save(self, output=None, delimiter=',', saveHeaders=True, replaceHeaders=None, trans=False):
        """saves the current state of the dictionary as a delimited text file"""
        # check output
        if not output:
            if not self.filename:
                raise PysheetException("No save target given!")
            else:
                output = self.filename
        # fix delimiter
        if delimiter == r'\t':
            delimiter = "\t"
        elif delimiter == r'\s':
            delimiter = "\s"
        # prepare the output writer
        if output == 'stdout':
            writer = csv.writer(sys.stdout, delimiter=delimiter)
        else:
            writer = csv.writer(open(output, "wb"), delimiter=delimiter)
        keys = self.rows.keys()
        keys.sort()
        skipAutoID = False
        skipAutoIDColumn = -1
        ret = []
        # check for Auto IDs
        if self.rows[self.HEADERS_ID][self.idColumn] == self.AUTO_ID_HEADER:
            skipAutoID = True # don't print auto ids!
            skipAutoIDColumn = self.idColumn
            if skipAutoIDColumn < 0:
                skipAutoIDColumn = len(self) + skipAutoIDColumn
        # set the header row on top first
        if saveHeaders:
            if skipAutoID:
                header = self.rows[self.HEADERS_ID][:self.idColumn] + \
                        self.rows[self.HEADERS_ID][(self.idColumn+1):]
            else:
                header = self.rows[self.HEADERS_ID]
            if replaceHeaders and len(replaceHeaders) != len(header):
                raise PysheetException(("Output headers given do not match number of "
                "output columns (%d)!\n%s") % (len(header), flatten(replaceHeaders, ", ")))
            elif replaceHeaders:
                header = replaceHeaders
            ret = [header]
        # now the content
        for k in keys:
            if k == self.HEADERS_ID:
                continue
            row = self.rows[k]
            line = []
            for col in range(len(row)): #i[row]:
                # if col == None: saves N.
                if skipAutoID and col == skipAutoIDColumn: # don't print auto ids!
                    continue
                if isinstance(row[col],str):
                    line.append(row[col])
                elif isNumber(row[col]):
                    line.append(str(row[col]))
                else:
                    line.append(cPickle.dumps(row[col]))
            ret.append(line)
        if trans:
            writer.writerows(transpose(ret))
        else:
            writer.writerows(ret)
        # set the filename
        if not self.filename:
            self.filename = output

    def isEmpty(self):
        """returns True if this sheet is blank"""
        return len(self)<=1 and self.height()<=1

    def isBlank(self, cell):
        """returns True if cell is considered blank"""
        if isList(cell):
            return reduce(lambda a,b: self.isBlank(a) and self.isBlank(b), cell)
            #len(filter((lambda i : not self.isBlank(i)) for i in cell)) == 0
        if cell in [None, '', self.BLANK_VALUE]:
            return True
        return False

    def __str__(self):
        """returns a string representation of this object"""
        if self.isEmpty():
            return "* empty *\n"
        try:
            from texttable import Texttable
        except ImportError:
            return "* module 'Texttable' is required to print stuff *\n"
        header = [[x.replace('__','') for x in self.getHeaders()]]
        ids = self.getIds()
        ids.sort()
        table = [self[i] for i in ids]
        table = header + table
        ttable = Texttable()#self.MAX_PRINT_WIDTH)
        ttable.set_deco(Texttable.HEADER | Texttable.VLINES)
        ttable.add_rows(table)
        try:
            return ttable.draw() + '\n'
        except ValueError as e:
            return "* table too wide to display; choose less then %d columns *\n" % len(self)

class PysheetException(Exception):
    """Pysheet exception class. You can raise it with a msg"""
    def __init__(self, msg, fname=None, line=0):
        if not fname:
            self.message = msg
        elif line and str(line).isdigit:
            self.message = "Error %s in file %s at %d" % (msg, fname, line)
        else:
            self.message = "Error %s in file %s" % (msg, fname)
        super(PysheetException, self).__init__(self.message)
    def __str__(self):
        return repr(self.message)

###############################
###### UTILITY FUNCTIONS ######
###############################

def readable(f):
    """type for argparse - checks that a file exists but does not open it"""
    if f.lower() == "stdin":
        return f.lower()
    f = os.path.realpath(f)
    if not os.path.isfile(f):
        raise argparse.ArgumentTypeError("File does not exist: %s" % f)
    return f

def writeable(f):
    """type for argparse - checks that a file is writeable but does not open it"""
    if f.lower() == "stdout":
        return f.lower()
    f = os.path.realpath(f)
    if os.path.isfile(f) and not os.access(f, os.W_OK):# or not os.access(os.path.dirname(f), os.W_OK):
        raise argparse.ArgumentTypeError("File is not writeable: %s" % f)
    return f

def yesNo(f):
    """type for argparse - receives yes|no and converts to True|False"""
    if f.lower() in ["y","yes","true","1"]:
        return True
    elif f.lower() in ["n","no","false","0"]:
        return False
    else:
        raise argparse.ArgumentTypeError("Value must be one of y|yes|true|1|n|no|false|0, not %s" % f)
    return f

def flatten(l, delim=" "):
    """converts an array to string with a delimiter in between items"""
    return reduce(lambda x,y: "%s%s%s" % (x,delim,y), l)

def printStackTrace():
    """prints an exception trace. To be used in an Except block"""
    traceback.print_exc(file=sys.stdout)

def isList(x):
    """returns True if x is some kind of a list"""
    return (not hasattr(x, "strip") and hasattr(x, "__getitem__") or hasattr(x, "__iter__"))

def isNumber(x, strOk=False):
    """returns True if x is a number (or list of numbers)"""
    if isList(x):
        return reduce(lambda a,b: isNumber(a, strOk=strOk) and isNumber(b, strOk=strOk), x)
        #len(filter((lambda i : not isNumber(i, strOk=strOk)),x)) == 0
    if strOk:
        x = tryNumber(x)
    return isinstance(x, (int, long, float, floating))

def tryNumber(x):
    """tries to return an appropriate number from x or just x if not a number"""
    if isList(x):
        return [tryNumber(i) for i in x]
    try:
        n = int(x) # will make long if long int!
        return n
    except ValueError:
        try:
            f = float(x)
            return f
        except ValueError:
            return x

def transpose(arr):
    """transposes a nested list (2D-array)"""
    return map(list, zip(*arr))

def unique(seq, blanks=True):
    """returns a list of unique elements from seq"""
    ret = []
    for e in seq:
        if e not in ret and (blanks or (not blanks and e)):
            ret.append(e)
    return ret

def clean(s):
    """returns the stripped lower-case of a string"""
    if isList(s):
        return [clean(i) for i in s]
    return (str(s).lower()).strip()

def sanitize(s):
    """removes special characters from a string"""
    return s # this takes too long so removing it
    if isList(s):
        return [sanitize(i) for i in s]
    p = re.compile('[\$\\\/&><\s\[\]\(\)\*:^]')
    return p.sub('_',str(s).strip())

def randomId(size=6, chars=None):
    """generates a random string of letters and numbers"""
    import random, string
    if not chars:
        chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for x in range(size))


if __name__ == '__main__':
    if PROFILE:
        import cProfile, pstats
        cProfile.runctx('main()', globals(), locals(), PROFILE_DAT)
        f = open(PROFILE_TXT, 'wb')
        for sort_key in 'time', 'cumulative':
            stats = pstats.Stats(PROFILE_DAT, stream=f)
            stats.sort_stats(sort_key)
            stats.print_stats(PROFILE_LIMIT)
            stats.strip_dirs()
            stats.sort_stats(sort_key)
            if sort_key == 'time':
                stats.print_callers(PROFILE_LIMIT)
            else:
                stats.print_callees(PROFILE_LIMIT)
        f.close()
        os.unlink(PROFILE_DAT)
    else:
        main()
