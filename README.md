Custom Data Entry automation Bot
it also automates the detection and correction of shop payment discrepancies in Google Sheets using the Google Sheets API. It ensures that recorded monthly payments match the expected payment values in each month's respective column.

## Features

-  Reads shop payment data from a CSV file.
-  Finds the shop row, makes neccesary calculations and fills the necessary months payment. 
-  Checks existing monthly payment entries in Google Sheets for discrepancies.
-  Compares them against the correct values stored in adjacent columns.
-  Automatically updates incorrect entries with the correct amount.
-  Uses batch updates for efficiency.
-  Logs and prints clear status messages for each operation.
