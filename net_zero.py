
def get_net_zero():
    import pandas as pd
    import numpy as np
    import requests
    from io import BytesIO
    from bs4 import BeautifulSoup
    from urllib import parse

    import warnings

    # ignore Warnings
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)

    #Get the Energy Generation Mix  for GB from ESO

    sql_query = '''SELECT "DATETIME", "GAS", "COAL", "NUCLEAR", "WIND", "HYDRO", "IMPORTS", "BIOMASS", "OTHER", "SOLAR" FROM  "f93d1835-75bc-43e5-84ad-12472b180a98" ORDER BY "_id" ASC '''
    params = {'sql': sql_query}
    
    response = requests.get("https://api.nationalgrideso.com/api/3/action/datastore_search_sql", params = parse.urlencode(params))
    data = response.json()["result"]
    gen_mix = pd.DataFrame(data["records"])
  
    gen_mix = (gen_mix
                    .astype({col: float for col in ['NUCLEAR', 'GAS', 'BIOMASS', 'COAL', 'OTHER', 'SOLAR','IMPORTS', 'WIND', 'HYDRO']})
                    .assign(GB_Renewable_Energy = lambda df: df.WIND+df.HYDRO+df.IMPORTS + df.BIOMASS +df.SOLAR,
                            GB_Non_Renewable_Energy = lambda df: df.GAS+ df.COAL,
                            Nuclear_Energy_et_al = lambda df: df.NUCLEAR+df.OTHER,
                            DATETIME = lambda df: pd.to_datetime(df.DATETIME).dt.tz_localize(None))
                    .rename(columns={'DATETIME': 'Year'})
                    .set_index('Year')
                    .loc[:,['GB_Renewable_Energy', 'GB_Non_Renewable_Energy', 'Nuclear_Energy_et_al']]
                    .resample('D').sum()
                )


    # Get the number of Households in the UK
    ECUK = pd.ExcelFile('ECUK_2022_Intensity_tables.xlsx')
    households = pd.read_excel(ECUK, 'Table I3', header=4, usecols=['Year', "No Households ('000s)"])

    households =(households
                .loc[lambda df: df.index <= 51]
                .assign(Year = lambda df: pd.PeriodIndex(pd.to_datetime(df.Year, format='%Y'), freq='D').to_timestamp())
                .set_index('Year')
                .rename(columns={"No Households ('000s)": 'Households'})
                )
    
    #Get the number of total renewable projects in the UK
    file_path = 'repd-january-2023.csv'
    repd = pd.read_csv(file_path, encoding='latin1', usecols=['Operator (or Applicant)', 'Development Status (short)', 'Operational'])
    renew_project = (repd
            # Drop null values
            .dropna(subset=['Operational'])
            # Extract the day the project became operational
            .assign(Year=lambda df: pd.PeriodIndex(df.Operational, freq='D').to_timestamp())
            # Limit to projects that are only operational
            .loc[(repd['Development Status (short)'] == 'Operational')]
            # Group by date and count the projects operational per year
            .groupby('Year')[['Operator (or Applicant)']].count()
            # Create a running total column
            .assign(Total_Renew_Projects=lambda df: df['Operator (or Applicant)'].cumsum())
            # Drop the Operator column
            .drop(columns=['Operator (or Applicant)'])
            # Resample to add missing days and fill NaN values with the previous non-NaN value
            .resample('D').last().fillna(method='ffill').astype(int)
            )

    #Get the Feed in Tariff Data
    
    # send a GET request to the URL and get the HTML response
    url="https://www.gov.uk/government/statistics/solar-photovoltaics-deployment"
    response2 = requests.get(url)

    # parse the HTML response using Beautiful Soup
    soup = BeautifulSoup(response2.text, "html.parser")

    # find the link to the Excel file
    excel_link_elem = soup.find("a", text="Solar photovoltaics deployment (Excel)")

    # get the href attribute of the link and read the Excel file into a pandas ExcelFile object
    excel_link = excel_link_elem.get("href")
    excel_content = requests.get(excel_link).content
    fit_doc = pd.ExcelFile(BytesIO(excel_content))

        

    # the sheet of interest is the seventh sheet
    fit_df = pd.read_excel(fit_doc, fit_doc.sheet_names[6], header=4)

    fit = (fit_df
           # Select the Total cumulative capacity generated in the UK
           .loc[fit_df.index == 43]
           # clean columns
           .rename(columns=lambda x: x.replace('\n', ''))
           .rename(columns=lambda x: x.replace('June', 'Jun'))
           # drop superfluous column
           .drop(columns=['CUMULATIVE CAPACITY (MW) [note 1]'])
           # remove extra spacing for proper conversion to datetime
           .rename(columns=lambda x: x.replace(' ', ''))
           # Transpose to set the date as the index
           .T
           # rename column
           .rename(columns={43: 'FiT_Deployments'})
           # convert index to datetime
           .pipe(lambda df: df.set_index((pd.to_datetime(df.index, format='%b%Y'))))
           .resample('D').sum()
           )
    
    #Get the Renewable Heat Incentive Data
    rhi_doc = pd.ExcelFile('RHI_monthly_official_stats_tables_Dec_22.xlsx')
    rhi_df = pd.read_excel(rhi_doc, 'M1.1', header=5)
    rhi = (rhi_df
           # select columns of interest
           .loc[:,['Year','Month','Cumulative number of accredited full applications']]
           # create new date time variable of the year and month
           .assign(Date=lambda df: pd.to_datetime((df.Year.map(str) + ' ' + df.Month)))
           # select the columns of interest
           .loc[:,['Date','Cumulative number of accredited full applications']]
           .rename(columns={'Date': 'Year', 'Cumulative number of accredited full applications': 'RHI_Deployments'})
           .set_index('Year')
           # filter out the first month since there was no installation
           .loc[lambda df: df.index > '2011-11-01']
           # change the frequency to daily for subsequent interpolation while merging
           .resample('D').sum()
          )
   
    #Get the Boiler Upgrade Scheme Data
    url2 = "https://www.gov.uk/government/collections/boiler-upgrade-scheme-statistics"
    response3 = requests.get(url2)
    soup2 = BeautifulSoup(response3.content, 'html.parser')

    #Find the first link in the Statistical Releases section
    statistical_releases_section = soup2.find('h3', {'class': 'group-title', 'id': 'statistical-releases'}).find_next('ul')
    statistical_release_link = statistical_releases_section.find('a')['href']
    statistical_release_link = 'https://www.gov.uk' + statistical_release_link

    #Go to the Statistical Release page
    response4 = requests.get(statistical_release_link)
    soup3 = BeautifulSoup(response4.content, 'html.parser')

    #Find the link to the Excel file in the Documents section
    document_section = soup3.find('section', {'id': 'documents'})
    h3_section = document_section.find('h3')
    excel_link2 = document_section.find('a', {'class': 'govuk-link'})['href']
    bus_doc=pd.ExcelFile(excel_link2)

    # the sheet of interest is the 12th sheet
    bus_df=pd.read_excel(bus_doc, bus_doc.sheet_names[11], header=5)

    bus =(bus_df
         #Select the Total redemptions paid
         .loc[bus_df.index ==15]
         #drop superfluous column
         .drop(columns=['Voucher status', 'Technology type', 'Total'])
         #remove extraspacing for proper conversion to datetime
         .rename(columns = lambda x: x.replace(' ', '')) 
         #Transpose to set the date as the index
         .T
          #rename column
         .rename(columns= {15: 'BUS_Deployments'})
         #convert index to datetime
         .pipe(lambda df: df.set_index((pd.to_datetime(df.index, format='%b%Y'))))
         .resample('D').sum()
        )
        
    #Merge the three incentives dataset together

    gov_incentive =(fit
                     .merge(rhi, left_index=True,right_index=True, how='outer').fillna(0)
                     .merge(bus, left_index=True,right_index=True, how='outer').fillna(0)
                     .assign(Total_Deployments = lambda df: df.FiT_Deployments +df.RHI_Deployments+df.BUS_Deployments)
                     .loc[:,['Total_Deployments']]
                     .replace(0,np.nan)
                     .resample('D').last().fillna(method='ffill').astype(int)
                    )

    # Get the Energy Generation Mix  for Northern Ireland
    url7 = "https://www.economy-ni.gov.uk/publications/northern-ireland-renewable-electricity-data-tables"
    response7 = requests.get(url7)

    # Parse HTML content using BeautifulSoup
    soup7 = BeautifulSoup(response7.content, 'html.parser')

    # Find the first <a> tag in the <div class="nigovfile clearfix"> section
    data_tables = soup7.find('div', class_='nigovfile clearfix')
    first_a = data_tables.find('a')
    # Get the href attribute of the first <a> tag
    download_url = first_a['href']
    # Load the Excel file into a pandas DataFrame
    ni_df = pd.read_excel(download_url, header=2)
    NI_gen_mix = (ni_df
                  # perform preliminary cleaning and selection
                  .drop(columns=['Unnamed: 0'])
                  .dropna(subset=['Unnamed: 1'], axis=0)
                  .reset_index(drop=True)
                  .loc[lambda df: df.index.isin([2, 3])]
                  .T
                  .pipe(lambda df: df.rename(columns=df.iloc[0]))
                  .iloc[1:]
                  # create a non_renewable energy variable and convert it to megawats from gigawatts
                  .assign(NI_Non_Renewable_Energy=lambda df: (df['Total Electricity Consumption (GWh)'] - df[
        'Total Renewable Electricity Generated (GWh)']) * 1000)
                  .rename(columns={'Total Renewable Electricity Generated (GWh)': 'NI_Renewable_Energy'})
                  # convert the renamed column to megawatt
                  .assign(NI_Renewable_Energy=lambda df: df.NI_Renewable_Energy * 1000)
                  .loc[:, ['NI_Renewable_Energy', 'NI_Non_Renewable_Energy']]
                  .rename(index=lambda x: pd.to_datetime(x))
                  .resample('D').last().interpolate(method='spline', order=1)
                  )
    #Get the label: Emissions Data
    filepath= 'total-ghg-emissions.csv'
    emission_df = pd.read_csv(filepath)

    emission = (emission_df
            .loc[(emission_df.Entity == 'United Kingdom')]
            .drop(columns=['Code', 'Entity'])
            .assign(Year=lambda df: pd.PeriodIndex(df.Year, freq='D').to_timestamp())
            .set_index('Year')
            .rename(columns={'Annual greenhouse gas emissions': 'Emissions'})
            )


    #Assemble the dataset
    net_zero = (gen_mix
    .merge(households,left_index=True, right_index=True, how='outer')
    .merge(renew_project,left_index=True, right_index=True, how='outer')
    .merge(gov_incentive,left_index=True, right_index=True, how='outer')
    .merge(NI_gen_mix,left_index=True, right_index=True, how='outer')
    .merge(emission,left_index=True, right_index=True, how='outer')
    #start when the government incentives begin
    .loc[lambda df: df.index >='2010-01-01']
    #fill the missing data with relevant methods
    .assign(Households = lambda df: df.Households.interpolate(method='spline', order=1).astype(int))
    .assign(Total_Deployments = lambda df: df.Total_Deployments.fillna(method='ffill'))
    .assign(Emissions = lambda df: df.Emissions.interpolate(method='spline', order=1))
    #Merge the Great Britain and Northern Ireland mix together and convert NI NAN values to 0
    .assign(Total_Renewable = lambda df: df.GB_Renewable_Energy + np.nan_to_num(df.NI_Renewable_Energy))
    .assign(Total_Non_Renewable = lambda df: df.GB_Non_Renewable_Energy + np.nan_to_num(df.NI_Non_Renewable_Energy))
    .dropna(subset=['GB_Renewable_Energy','GB_Non_Renewable_Energy','Nuclear_Energy_et_al'], axis=0)
    #select the necessary columns and re-arrange
    .loc[:,['Total_Renewable', 'Total_Non_Renewable', 'Nuclear_Energy_et_al', 'Households', 'Total_Renew_Projects','Total_Deployments','Emissions']]
    )
 

    #Save the dataset
    return net_zero.to_csv('net_zero.csv')

get_net_zero()