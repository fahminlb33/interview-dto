import gspread
import pandas as pd

from dagster import ConfigurableResource


class GoogleSheetResource(ConfigurableResource):
    sheet_id: str
    google_application_credentials: str

    def read(self, sheet_num: int) -> pd.DataFrame:
        # authenticate with google sheets
        gsheet = gspread.service_account(filename=self.google_application_credentials)

        # open spreadsheet and worksheet
        spreadsheet = gsheet.open_by_key(self.sheet_id)
        worksheet = spreadsheet.get_worksheet(sheet_num)

        # get all records
        records = worksheet.get_all_records()

        # convert to dataframe
        return pd.DataFrame(records)

    def write(self, sheet: int, df: pd.DataFrame) -> None:
        # authenticate with google sheets
        gsheet = gspread.service_account(filename=self.google_application_credentials)

        # open spreadsheet and worksheet
        spreadsheet = gsheet.open_by_key(self.sheet_id)
        worksheet = spreadsheet.get_worksheet(sheet)

        # update worksheet
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())

