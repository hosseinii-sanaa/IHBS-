#' Add attributes to HBSIR data
#'
#' This function adds province, county, and urban/rural attributes to the data frame based on ID and year.
#'
#' @param df A data frame with 'ID' and 'year' columns.
#'
#' @return The data frame with added 'Province', 'County', 'Urban_Rural' columns.
#'
#' @export
#'
#' @importFrom arrow read_parquet
hbsir.add_attributes <- function(df) {
  df$ID <- as.character(df$ID)
  #Load external data with error handling
  external_url <- "https://s3.ir-tbz-sh1.arvanstorage.ir/iran-open-data/EXTERNAL/hbsir_counties.parquet"
  tryCatch({
    external_data <- arrow::read_parquet(external_url)
  }, error = function(e) {
    stop("Failed to load external data: ", e$message)
  })
  #Check required columns in external_data
  if (!all(c("ID", "Region_Code") %in% colnames(external_data))) {
    stop("External data is missing required columns: ID, Region_Code")
  }
  #Define province codes and corresponding names
  province_codes <- c(
    "00" = "Markazi", "01" = "Gilan", "02" = "Mazandaran",
    "03" = "East_Azerbaijan", "04" = "West_Azerbaijan", "05" = "Kermanshah",
    "06" = "Khuzestan", "07" = "Fars", "08" = "Kerman", "09" = "Razavi_Khorasan",
    "10" = "Isfahan", "11" = "Sistan_and_Baluchestan", "12" = "Kurdistan",
    "13" = "Hamadan", "14" = "Chaharmahal_and_Bakhtiari", "15" = "Lorestan",
    "16" = "Ilam", "17" = "Kohgiluyeh_and_Boyer_Ahmad", "18" = "Bushehr",
    "19" = "Zanjan", "20" = "Semnan", "21" = "Yazd", "22" = "Hormozgan",
    "23" = "Tehran", "24" = "Ardabil", "25" = "Qom", "26" = "Qazvin",
    "27" = "Golestan", "28" = "North_Khorasan", "29" = "South_Khorasan", "30" = "Alborz"
  )
  #Define the county codes and corresponding names based on the provided data
  county_codes <- c(
    "0001" = "Arak",
    "0002" = "Ashtian",
    "0003" = "Tafresh",
    "0004" = "Khomein",
    "0005" = "Delijan",
    "0006" = "Saveh",
    "0007" = "Shazand",
    "0009" = "Mahallat",
    "0010" = "Zarandieh",
    "0011" = "Komeijan",
    "0012" = "Khondab",
    "0013" = "Farahan",
    "0101" = "Astara",
    "0102" = "Astane-ye-Ashrafiyeh",
    "0103" = "Bandar-e-Anzali",
    "0104" = "Talesh",
    "0105" = "Rasht",
    "0106" = "Rudbar",
    "0107" = "Rudsar",
    "0108" = "Some'e-Sara",
    "0109" = "Fuman",
    "0110" = "Langrud",
    "0111" = "Lahijan",
    "0112" = "Shaft",
    "0113" = "Amlash",
    "0114" = "Rezvanshahr",
    "0115" = "Siahkal",
    "0116" = "Masal",
    "0201" = "Amol",
    "0202" = "Babol",
    "0204" = "Behshahr",
    "0205" = "Tonekabon",
    "0206" = "Ramsar",
    "0207" = "Sari",
    "0208" = "Savadkuh",
    "0210" = "Qaemshahr",
    "0214" = "Nur",
    "0215" = "Noshahr",
    "0216" = "Babolsar",
    "0218" = "Mahmudabad",
    "0219" = "Neka",
    "0220" = "Chalus",
    "0221" = "Juybar",
    "0222" = "Galugah",
    "0223" = "Feredunkenar",
    "0224" = "Abbasabad",
    "0225" = "Miandorud",
    "0226" = "Simorgh",
    "0227" = "Northern_Savadkooh",
    "0228" = "Kelardasht",
    "0302" = "Ahar",
    "0303" = "Tabriz",
    "0305" = "Sarab",
    "0306" = "Maragheh",
    "0307" = "Marand",
    "0310" = "Mianeh",
    "0311" = "Hashtrud",
    "0312" = "Bonab",
    "0313" = "Bostanabad",
    "0314" = "Shabestar",
    "0315" = "Kalibar",
    "0316" = "Heris",
    "0319" = "Jolfa",
    "0320" = "Malekan",
    "0321" = "Azarshahr",
    "0322" = "Osku",
    "0323" = "Charuymaq",
    "0324" = "Varzaqan",
    "0325" = "Ajabshir",
    "0326" = "Khodaafarin",
    "0401" = "Urumia",
    "0402" = "Piranshahr",
    "0403" = "Khoy",
    "0404" = "Sardasht",
    "0405" = "Salmas",
    "0406" = "Maku",
    "0407" = "Mahabad",
    "0408" = "Miandoab",
    "0409" = "Naqadeh",
    "0410" = "Bukan",
    "0411" = "Shahindej",
    "0412" = "Takab",
    "0413" = "Oshnaviyeh",
    "0414" = "Chaldoran",
    "0415" = "Poldasht",
    "0416" = "Chaypareh",
    "0417" = "Showt",
    "0501" = "Islamabad-e-Gharb",
    "0502" = "Kermanshah",
    "0503" = "Paveh",
    "0504" = "Sarpol-e-Zahab",
    "0505" = "Sonqor",
    "0506" = "Qasr-e-Shirin",
    "0507" = "Kangavar",
    "0508" = "Gilan-e-Gharb",
    "0509" = "Javanrud",
    "0510" = "Sahneh",
    "0511" = "Harsin",
    "0512" = "Solas-e-Babajani",
    "0513" = "Dalaho",
    "0514" = "Ravansar",
    # Khuzestan
    "0601" = "Abadan",
    "0602" = "Andimeshk",
    "0603" = "Ahvaz",
    "0604" = "Izeh",
    "0605" = "Mahshahr",
    "0606" = "Behbahan",
    "0607" = "Khorramshahr",
    "0608" = "Dezful",
    "0609" = "Dasht-e-Azadegan",
    "0610" = "Ramhormoz",
    "0611" = "Shadegan",
    "0612" = "Shushtar",
    "0613" = "Masjed-Soleyman",
    "0614" = "Shush",
    "0615" = "Baghmalek",
    "0616" = "Omidiyeh",
    "0617" = "Lali",
    "0618" = "Hendijan",
    "0619" = "Ramshir",
    "0620" = "Guotvand",
    "0621" = "Andika",
    "0622" = "Haftgol",
    "0623" = "Hoveizeh",
    "0624" = "Bavi",
    "0625" = "Hamidieh",
    "0626" = "Aghajari",
    "0627" = "Karoun",

    # Fars
    "0701" = "Abadeh",
    "0702" = "Estahban",
    "0703" = "Eqlid",
    "0704" = "Jahrom",
    "0705" = "Darab",
    "0706" = "Sepidan",
    "0707" = "Shiraz",
    "0708" = "Fasa",
    "0709" = "Firuzabad",
    "0710" = "Kazerun",
    "0711" = "Larestan",
    "0712" = "Marvdasht",
    "0713" = "Mamasani",
    "0714" = "Neyriz",
    "0715" = "Lamerd",
    "0716" = "Bavanat",
    "0717" = "Arsanjan",
    "0718" = "Khorrambid",
    "0719" = "Zarrindasht",
    "0720" = "Qir-o-Karzin",
    "0721" = "Mehr",
    "0722" = "Farashband",
    "0723" = "Pasargad",
    "0724" = "Khonj",
    "0725" = "Sarvestan",
    "0726" = "Rostam",
    "0727" = "Gerash",
    "0728" = "Kavar",
    "0729" = "Kherameh",

    # Kerman
    "0801" = "Baft",
    "0802" = "Bam",
    "0803" = "Jiroft",
    "0804" = "Rafsanjan",
    "0805" = "Zarand",
    "0806" = "Sirjan",
    "0807" = "Shahr-e-Babak",
    "0808" = "Kerman",
    "0809" = "Kahnuj",
    "0810" = "Bardsir",
    "0811" = "Ravar",
    "0812" = "Anbarabad",
    "0813" = "Manujan",
    "0814" = "Kuhbonan",
    "0815" = "Roudbar-e-Jonub",
    "0816" = "Ghaleye-Ganj",
    "0817" = "Rigan",
    "0818" = "Rabar",
    "0819" = "Fahraj",
    "0820" = "Anar",
    "0821" = "Normashir",
    "0822" = "Faryab",
    "0823" = "Arzuiyeh",
    # Razavi Khorasan
    "0901" = "Esfarayen",
    "0902" = "Bojnurd",
    "0903" = "Birjand",
    "0904" = "Taybad",
    "0905" = "Torbat-e-Heydarieh",
    "0906" = "Torbat-e-Jam",
    "0907" = "Dargaz",
    "0908" = "Sabzevar",
    "0909" = "Shirvan",
    "0910" = "Tabas",
    "0911" = "Ferdows",
    "0912" = "Qaen",
    "0913" = "Quchan",
    "0914" = "Kashmar",
    "0915" = "Gonabad",
    "0916" = "Mashhad",
    "0917" = "Nishapur",
    "0918" = "Chenaran",
    "0919" = "Khaf",
    "0920" = "Sarakhs",
    "0921" = "Nehbandan",
    "0922" = "Fariman",
    "0923" = "Bardaskan",
    "0924" = "Jajrom",
    "0925" = "Maneh-o-Samalqan",
    "0927" = "Rashtkhar",
    "0928" = "Kalat",
    "0929" = "Khalilabad",
    "0930" = "Mahvelat",
    "0931" = "Bajestan",
    "0932" = "Torghabe-o-Shandiz",
    "0933" = "Firuzeh",
    "0934" = "Joghatai",
    "0935" = "Zave",
    "0936" = "Jowayin",
    "0937" = "Bakharz",
    "0938" = "Khoshab",
    "0939" = "Davarzan",

    # Isfahan
    "1001" = "Ardestan",
    "1002" = "Isfahan",
    "1003" = "Khomeinishahr",
    "1004" = "Khansar",
    "1005" = "Semirom",
    "1006" = "Faridan",
    "1007" = "Fereydunshahr",
    "1008" = "Falavarjan",
    "1009" = "Shahreza",
    "1010" = "Kashan",
    "1011" = "Golpayegan",
    "1012" = "Lenjan",
    "1013" = "Nain",
    "1014" = "Najafabad",
    "1015" = "Natanz",
    "1016" = "Shahinshahro Meymeh",
    "1017" = "Mobarakeh",
    "1018" = "Aran-o-Bidgol",
    "1019" = "Tiran-o-Korun",
    "1020" = "Chadegan",
    "1021" = "Dehaghan",
    "1022" = "Borkhar",
    "1023" = "Khorobiyabanak",
    "1024" = "Booeino_Miyandasht",
    "1101" = "Iranshahr",
    "1102" = "Chabahar",
    "1103" = "Khash",
    "1104" = "Zabol",
    "1105" = "Zahedan",
    "1106" = "Saravan",
    "1107" = "Nikshahr",
    "1108" = "Sarbaz",
    "1109" = "Konarak",
    "1110" = "Zehak",
    "1111" = "Hirmand",
    "1112" = "Dalgan",
    "1113" = "Mehrestan",
    "1114" = "Sibo_Soran",
    "1115" = "Nimrouz",
    "1116" = "Hamoun",
    "1117" = "Mirjaveh",
    "1118" = "Ghasre_Ghand",
    "1119" = "Fanouj",

    "1201" = "Baneh",
    "1202" = "Bijar",
    "1203" = "Saqqez",
    "1204" = "Sannandaj",
    "1205" = "Qorveh",
    "1206" = "Marivan",
    "1207" = "Divandarreh",
    "1208" = "Kamyaran",
    "1209" = "Sarvabad",
    "1210" = "Dehgolan",
    # Hamadan Province
    "1301" = "Tuyserkan",
    "1302" = "Malayer",
    "1303" = "Nahavand",
    "1304" = "Hamadan",
    "1305" = "Kabudarahang",
    "1306" = "Asadabad",
    "1307" = "Bahar",
    "1308" = "Razan",
    "1309" = "Famenin",

    # Chaharmahal and Bakhtiari Province
    "1401" = "Borujen",
    "1402" = "Shahrekord",
    "1403" = "Farsan",
    "1404" = "Lordegan",
    "1405" = "Ardal",
    "1406" = "Kuhrang",
    "1407" = "Kiaar",
    "1408" = "Saman",
    "1409" = "Ben",

    # Lorestan Province
    "1501" = "Aligudarz",
    "1502" = "Borujerd",
    "1503" = "Khorramabad",
    "1504" = "Delfan",
    "1505" = "Dorud",
    "1506" = "Kuhdasht",
    "1507" = "Azna",
    "1508" = "Poldokhtar",
    "1509" = "Selseleh",
    "1510" = "Doureh",
    "1511" = "Romeshkan",

    # Ilam Province
    "1601" = "Ilam",
    "1602" = "Darreh Shahr",
    "1603" = "Dehloran",
    "1604" = "Shirvan-o-Chardavol",
    "1605" = "Mehran",
    "1606" = "Abdanan",
    "1607" = "Eyvan",
    "1608" = "Malekshahi",
    "1609" = "Sirvan",
    "1610" = "Badre",

    # Kohgiluyeh and Boyer-Ahmad Province
    "1701" = "Boyer-Ahmad",
    "1702" = "Kohgeluyeh",
    "1703" = "Gachsaran",
    "1704" = "Dena", #
    "1705" = "Bahmai",
    "1706" = "Charam",
    "1707" = "Basht",
    "1708" = "Landeh",

    # Bushehr Province
    "1801" = "Bushehr",
    "1802" = "Tangestan",
    "1803" = "Dashtestan",
    "1804" = "Dashti",
    "1805" = "Dayyer",
    "1806" = "Kangan",
    "1807" = "Ganaveh",
    "1808" = "Deylam",
    "1809" = "Jam",
    "1810" = "Asaluyeh",

    # Zanjan Province
    "1901" = "Abhar",
    "1903" = "Khodabandeh",
    "1904" = "Zanjan",
    "1906" = "Eejrud",
    "1907" = "Khorramdarreh",
    "1908" = "Tarom",
    "1909" = "Mahneshan",
    "1910" = "Soltanieh",

    # Semnan Province
    "2001" = "Damghan",
    "2002" = "Semnan",
    "2003" = "Shahrud",
    "2004" = "Garmsar",
    "2005" = "Mehdishahr",
    "2006" = "Aradan",
    "2007" = "Meyami",
    "2008" = "Sorkheh",
    # Yazd Province
    "2101" = "Ardekan",
    "2102" = "Bafq",
    "2103" = "Taft",
    "2104" = "Mehriz",
    "2105" = "Yazd",
    "2106" = "Meybod",
    "2107" = "Abarkuh",
    "2108" = "Ashkezar",
    "2109" = "Khatam",
    "2110" = "Tabas",
    "2111" = "Bahabad",
    # Hormozgan Province
    "2201" = "Abumusa",
    "2202" = "Bandar-Lengeh",
    "2203" = "Bandar-Abbas",
    "2204" = "Qeshm",
    "2205" = "Minab",
    "2206" = "Jask",
    "2207" = "Rudan",
    "2208" = "Hajiabad",
    "2209" = "Bastak",
    "2210" = "Khamir",
    "2211" = "Parsian",
    "2212" = "Sirik",
    "2213" = "Bashagard",
    # Tehran Province
    "2301" = "Tehran",
    "2302" = "Damavand",
    "2303" = "Rey",
    "2304" = "Shemiranat",
    "2305" = "Karaj",
    "2306" = "Varamin",
    "2308" = "Savojbolagh",
    "2309" = "Shahriar",
    "2310" = "Islamshahr",
    "2312" = "Robat-Karim",
    "2313" = "Pakdasht",
    "2314" = "Firuzkuh",
    "2315" = "Nazarabad",
    "2316" = "Shahr-e_Qods",
    "2317" = "Malard",
    "2318" = "Pishva",
    "2319" = "Baharestan",
    "2320" = "Pardis",
    "2321" = "Qarchak",
    # Ardabil Province
    "2401" = "Ardabil",
    "2402" = "Bilasavar",
    "2403" = "Khalkhal",
    "2404" = "Meshginshahr",
    "2405" = "Germi",
    "2406" = "Parsabad",
    "2407" = "Kowsar",
    "2408" = "Namin",
    "2409" = "Nir",
    "2410" = "Sareyn",
    # Qom Province
    "2501" = "Qom",
    # Qazvin Province
    "2601" = "Buin-Zahra",
    "2602" = "Takestan",
    "2603" = "Qazvin",
    "2604" = "Abyek",
    "2605" = "Alborz",
    "2606" = "Avaj",
    # Golestan Province
    "2701" = "Bandar-e-Gaz",
    "2702" = "Bandar-e-Torkaman",
    "2703" = "Aliabad",
    "2704" = "Kordkuy",
    "2705" = "Gorgan",
    "2706" = "Gonbad-e-Kavus",
    "2707" = "Minudasht",
    "2708" = "Aqqala",
    "2709" = "Kolaleh",
    "2710" = "Azadshahr",
    "2711" = "Ramian",
    "2712" = "Maravehtapeh",
    "2713" = "Gomishan",
    "2714" = "Galikash",
    # North Khorasan Province
    "2801" = "Esfarayen",
    "2802" = "Bojnurd",
    "2803" = "Jajrom",
    "2804" = "Shirvan",
    "2805" = "Faroj",
    "2806" = "Maneh-o-Samalqan",
    "2807" = "Germeh",
    "2808" = "Razo_Jalgelan",
    "2809" = "Shirvan",
    "2813" = "Faroj",
    "2824" = "Jajrom",
    "2825" = "Maneh-o-Samalqan",
    # South Khorasan Province
    "2901" = "Birjand",
    "2902" = "Darmian",
    "2903" = "Birjand",
    "2904" = "Qaen",
    "2905" = "Nehbandan",
    "2906" = "Sarayan",
    "2907" = "Ferdows",
    "2908" = "Boshruyeh",
    "2909" = "Tabas",
    "2910" = "Khusf",
    "2911" = "Sarayan",
    "2912" = "Qaen",
    "2921" = "Nehbandan",
    # Alborz Province
    "3001" = "Karaj",
    "3002" = "Savojbolagh",
    "3003" = "Nazarabad",
    "3004" = "Taleghan",
    "3005" = "Eshtehard",
    "3006" = "Fardis"
  )
  #Ensure 'ID' has no missing values
  if (any(is.na(df$ID))) {
    stop("The 'ID' column contains missing values.")
  }
  #Initialize columns
  df$Province <- NA
  df$County <- NA
  df$Urban_Rural <- NA
  #Loop over each row
  for (i in 1:nrow(df)) {
    id_value <- df$ID[i]
    province_code <- substr(id_value, 2, 3)
    county_code <- substr(id_value, 2, 5)
    first_digit <- substr(id_value, 1, 1)
    #Debugging: Log ID and extracted codes
    message("Processing ID: ", id_value, ", Province Code: ", province_code, ", County Code: ", county_code)
    #Assign Urban/Rural
    if (df$year[i] >= 1363 && df$year[i] <= 1386) {
      if (first_digit == "0") {
        df$Urban_Rural[i] <- "Rural"
      } else if (first_digit == "1") {
        df$Urban_Rural[i] <- "Urban"
      }
    } else {
      if (first_digit == "1") {
        df$Urban_Rural[i] <- "Rural"
      } else if (first_digit == "2") {
        df$Urban_Rural[i] <- "Urban"
      }
    }
    #Assign Province and County
    if (df$year[i] >= 1387 && df$year[i] <= 1391) {
      #Use external data for 13871391
      formatted_county_code <- sprintf("%04d", as.integer(county_code))
      matched_row <- external_data[external_data$ID == formatted_county_code, ]
      if (nrow(matched_row) > 0) {
        region_code <- matched_row$Region_Code[1]
        prov_code <- substr(region_code, 2, 3)
        cnty_code <- substr(region_code, 2, 5)
        if (prov_code %in% names(province_codes)) {
          df$Province[i] <- province_codes[prov_code]
        } else {
          message("Province code not found in external data: ", prov_code)
        }
        if (cnty_code %in% names(county_codes)) {
          df$County[i] <- county_codes[cnty_code]
        } else {
          message("County code not found in external data: ", cnty_code)
        }
      } else {
        message("No match found for county_code: ", formatted_county_code)
      }
    } else {
      #Use direct mapping for years outside 13871391
      if (province_code %in% names(province_codes)) {
        df$Province[i] <- province_codes[province_code]
      } else {
        message("Province code not found in mappings: ", province_code)
      }
      if (county_code %in% names(county_codes)) {
        df$County[i] <- county_codes[county_code]
      } else {
        message("County code not found in mappings: ", county_code)
      }
    }
  }
  return(df)
}
