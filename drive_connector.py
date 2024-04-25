from google.colab import auth
from google.auth import default
from googleapiclient.discovery import build
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd


class DriveConnector:

  _BASE_URL = 'https://docs.google.com/spreadsheets/d/'

  def __init__(self, drive_id):
    self._drive_id = drive_id
    self._service = build('drive', 'v3')
    self._service_doc = build('docs', 'v1')
    self._creds, _ = default()
    auth.authenticate_user()


  def _get_folder_id(self, folder_name):

    response = self._service.files().list(q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'",
                                    spaces='drive',
                                    driveId=self._drive_id,
                                    corpora="drive",
                                    includeItemsFromAllDrives=True,
                                    supportsAllDrives=True,
                                    fields='files(id, name)').execute()

    for folder in response.get('files', []):
        return folder.get('id')

    return None


  def _get_file_id(self, file_name, folder_id=None):

    query = f"name='{file_name}'" + (f" and '{folder_id}' in parents" if folder_id!=None else "")
    response = self._service.files().list(q=query,
                                  spaces='drive',
                                  driveId=self._drive_id,
                                  corpora="drive",
                                  includeItemsFromAllDrives=True,
                                  supportsAllDrives=True,
                                  fields='files(id, name)').execute()

    for file in response.get('files', []):
        return file.get('id')

    return None


  def get_gsheet(self, file_name, folder_name=None):

    if folder_name is None:
      folder_id = None
    else:
      folder_id = self._get_folder_id(folder_name)
    file_id = self._get_file_id(file_name, folder_id)

    gc = gspread.authorize(self._creds)
    spreadsheet_url = self._BASE_URL+file_id
    spreadsheet = gc.open_by_url(spreadsheet_url)

    return spreadsheet

  def get_gsheet_as_df(self, file_name, folder_name=None, sheet=None):

    spreadsheet = self.get_gsheet(file_name, folder_name=folder_name)

    if sheet is not None:
      worksheet = spreadsheet.worksheet(sheet)
      df = self.gsheet_to_df(worksheet)
    else:
      df = pd.DataFrame()
      for worksheet in spreadsheet.worksheets():
        df_tmp = self.gsheet_to_df(worksheet)
        df = pd.concat([df, df_tmp], ignore_index=True)
    return df


  def gsheet_to_df(self, sheet):
    data = sheet.get_all_values()
    df = pd.DataFrame(data)
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    df.reset_index(drop=True, inplace=True)
    return df

  def write_gsheet(self, file_name, df, sheet_name=None, folder_name=None,):
    spreadsheet = self.get_gsheet(file_name, folder_name=folder_name)
    if sheet_name==None:
      worksheet = spreadsheet.sheet1
    else:
      worksheet = spreadsheet.worksheet(sheet_name)
    set_with_dataframe(worksheet, df)
    return self._BASE_URL+spreadsheet.id


  def get_gdoc(self, file_name, folder_name=None):
    if folder_name is None:
      folder_id = None
    else:
      folder_id = self._get_folder_id(folder_name)
    file_id = self._get_file_id(file_name, folder_id)
    document = self._service_doc.documents().get(documentId=file_id).execute()
    doc = document.get('body').get('content')
    return doc


  def get_gdoc_as_txt(self, file_name, folder_name=None):
    doc = self.get_gdoc(file_name, folder_name)
    return self._read_paragraph_element(doc)

  def _read_paragraph_element(self, elements):
    text = ""
    for element in elements:
        if 'textRun' in element:
            text += element['textRun']['content']
        elif 'paragraph' in element:
            text += self._read_paragraph_element(element['paragraph']['elements'])
    return text

