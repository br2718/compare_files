# compare_files
Compares two CSV files by the fields that you specify

0.	Two sample files have been included for you.  Download all files into the same directory and 
	run from the command line with "python compare_files.py config.json" (without quotes).
	You can also follow the directions below to create and run your own CSV files.
1.	Update the config.json file with two different CSV file names.  To sample files have been included for you.
	Start the path with "." and include subdirectories if needed.
2. 	If your file has a header, make hasHeader true; otherwise hasHeader should be false.
	One file can have a header while the other one doesn't, both files can have headers, or neither file can have headers.
3.  If you want to include the header in the output, make includeHeader true; otherwise, includeHeader should be false.
4.	For each file, enter the "columnsToCompare," and number the columns starting from zero.
	The column numbers in each file do not have to be the same, but the data types should be the same.
5.	For "columnsToSortBy" enter the sort columns, by numbering each number in "columnsToCompare" starting from zero.
	For example, if the "columnsToCompare" are [0,2,4], the "columnsToSortBy" will be [0,1,2].
6.	Lastly, specify the directory for the output files.
	Start the path with "." and include subdirectories if needed.
	To put your files in the working directory, specify "".
