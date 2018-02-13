The is Malcolm Davidson's submission for the insight data engineering program

Dependencies: Pandas Library, Math, and Sys

Dveloped on: Ubuntu 16.04 - 2008 Macbook Unibody

Goal: Develop a program to exreact information on repeat donors to political campaigns

Approach: I developed a single python2 program to carry out the task. The file ould be broken into modular
fragments, but this seemed unnecesary here. I also favored writing out each code for readability at the cost of more 'pythonic'
style of coding.

Since we had to emulate a streaming program I decided that my program would work around a constant update approach. This means that analysis of repeat donors occurs uppon each import.

The program functions by first opening and extracting the desired percentage. This is followed by the open_file function. Open_file opens itcont.txt and parses the lines one by one. When a line is imported it is converted into a Panda's data frame (df) to facilitate slicing. The raw, single row, df is then passed to a function to check for empty or malformed features; after which unecessary features are pruned. The process is repeated for each line of the file.

When a line is imported, it is checked against an internal cache to see if the donor has made a previous contribution. If this proves true, the record is set aside into a repeat cache. Before the next import, the repeat cache is analyzed to compute percentiles, total contributions, number of contributions, and the year. An export is prepared and sent to the "repeat_donors.txt" file. It shold be noted that an interem unique identifier is built from the zip code, campaign ID, and year. This allows grouping of repeat donors for quick analysis.


