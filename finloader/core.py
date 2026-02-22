import pandas as pd


class ForexSymbol:
    CURRENCIES = {
        'AED': 'UAE Dirham',
        'AFN': 'Afghanistan Afghani',
        'ALL': 'Albanian Lek',
        'AMD': 'Armenian Dram',
        'AOA': 'Angolan kwanza',
        'ARS': 'Argentinian Peso',
        'AUD': 'Australian Dollar',
        'AWG': 'Aruban Florin',
        'AZN': 'Azerbaijani Manat',
        'BAM': 'Bosnian and Herzegovina Convertible Mark',
        'BBD': 'Barbadian Dollar',
        'BDT': 'Bangladeshi Taka',
        'BGN': 'Bulgarian Lev',
        'BHD': 'Baharain Dinar',
        'BIF': 'Burundian Franc',
        'BMD': 'Bermudian Dollar',
        'BND': 'Brunei Dollar',
        'BOB': 'Bolivian Boliviano',
        'BRL': 'Brazil Real',
        'BSD': 'Bahamas Dollar',
        'BTN': 'Bhutanese Ngultrum',
        'BWP': 'Botswana Pula',
        'BYN': 'Belarusian Ruble',
        'BZD': 'Belize Dollar',
        'CAD': 'Canadian Dollar',
        'CDF': 'Congolese Franc',
        'CHF': 'Swiss Franc',
        'CLP': 'Chilean Peso',
        'CNH': 'Chinese Yuan (Offshore)',
        'CNY': 'Chinese Yuan',
        'COP': 'Colombian Peso',
        'CRC': 'Costa Rican Colon',
        'CUP': 'Cuban Peso',
        'CVE': 'Cape Verdean Escudo',
        'CZK': 'Czech Koruna',
        'DJF': 'Djiboutian Franc',
        'DKK': 'Danish Krone',
        'DOP': 'Dominican Peso',
        'DZD': 'Algerian Dinar',
        'EGP': 'Egyptian Pound',
        'ETB': 'Ethiopian Birr',
        'EUR': 'Euro',
        'FJD': 'Fiji Dollar',
        'FKP': 'Falkland Islands Pound',
        'GBP': 'British Pound',
        'GEL': 'Georgian lari',
        'GHS': 'Ghanaian Cedi',
        'GIP': 'Gibraltar Pound',
        'GMD': 'Gambian Dalasi',
        'GNF': 'Guinean Franc',
        'GTQ': 'Guatemalan Quetzal',
        'GYD': 'Guyanese Dollar',
        'HKD': 'Hong Kong Dollar',
        'HNL': 'Honduran Lempira',
        'HRK': 'Croatian Kuna',
        'HTG': 'Haitian Gourde',
        'HUF': 'Hungarian Forint',
        'IDR': 'Indonesian Rupiah',
        'ILS': 'Israeli Shekel',
        'INR': 'Indian Rupee',
        'IQD': 'Iraqi Dinar',
        'IRR': 'Iranian Rial',
        'ISK': 'Icelandic Krona',
        'JMD': 'Jamaican Dollar',
        'JOD': 'Jordan Dinar',
        'JPY': 'Japanese Yen',
        'KES': 'Kenyan Shilling',
        'KGS': 'Kyrgyzstan som',
        'KHR': 'Cambodian Riel',
        'KMF': 'Comorian Franc',
        'KRW': 'Korean Won',
        'KWD': 'Kuwaiti Dinar',
        'KYD': 'Cayman Islands Dollar',
        'KZT': 'Kazakh Tenge',
        'LAK': 'Lao Kip',
        'LBP': 'Lebanese Pound',
        'LKR': 'Sri Lankan Rupee',
        'LRD': 'Liberian Dollar',
        'LSL': 'Lesotho loti',
        'LYD': 'Libyan Dinar',
        'MAD': 'Moroccan Dirham',
        'MDL': 'Moldovan Leu',
        'MGA': 'Malagasy Ariary',
        'MKD': 'Macedonian Denar',
        'MMK': 'Myanmar kyat',
        'MNT': 'Mongolian Tugrik',
        'MOP': 'Macanese Pataca',
        'MRU': 'Mauritanian Ouguiya',
        'MUR': 'Mauritian Rupee',
        'MVR': 'Maldivian Rufiyaa',
        'MWK': 'Malawian Kwacha',
        'MXN': 'Mexican Peso',
        'MYR': 'Malaysian Ringgit',
        'MZN': 'Mozambican Metical',
        'NAD': 'Namibian Dollar',
        'NGN': 'Nigerian Naira',
        'NIO': 'Nicaraguan Cordoba',
        'NOK': 'Norwegian Krone',
        'NPR': 'Nepalese Rupee',
        'NZD': 'New Zealand Dollar',
        'OMR': 'Omani Rial',
        'PAB': 'Panamanian Balboa',
        'PEN': 'Peru Sol',
        'PGK': 'Papua New Guinean kina',
        'PHP': 'Philippine Peso',
        'PKR': 'Pakistani Rupee',
        'PLN': 'Polish Zloty',
        'PYG': 'Paraguayan Guarani',
        'QAR': 'Qatari Riyal',
        'RON': 'Romanian Leu',
        'RSD': 'Serbian Dinar',
        'RUB': 'Russian Ruble',
        'RWF': 'Rwandan Franc',
        'SAR': 'Saudi Riyal',
        'SBD': 'Solomon Islands Dollar',
        'SCR': 'Seychelles Rupee',
        'SDG': 'Sudanese Pound',
        'SDR': 'Special Drawing Rights',
        'SEK': 'Swedish Krona',
        'SGD': 'Singapore Dollar',
        'SHP': 'Saint Helena Pound',
        'SLE': 'Sierra Leonean Leone',
        'SOS': 'Somali Shilling',
        'SRD': 'Surinamese Dollar',
        'STN': 'São Tomé and Príncipe Dobra',
        'SVC': 'El Salvador Colon',
        'SYP': 'Syrian Pound',
        'SZL': 'Swazi Lilangeni',
        'THB': 'Thai Baht',
        'TJS': 'Tajikistani somoni',
        'TMT': 'Turkmenistan manat',
        'TND': 'Tunisian Dinar',
        'TOP': 'Tongan Paʻanga',
        'TRY': 'Turkish Lira',
        'TTD': 'Trinidad Dollar',
        'TWD': 'Taiwan Dollar',
        'TZS': 'Tanzanian Shilling',
        'UAH': 'Ukrainian Hryvnia',
        'UGX': 'Ugandan Shilling',
        'USD': 'US Dollar',
        'UYU': 'Uruguayan Peso',
        'UZS': 'Uzbekistani Sum',
        'VEF': 'Venezuelan Bolivar',
        'VES': 'Venezuelan Bolívar Soberano',
        'VND': 'Vietnamese Dong',
        'VUV': 'Vanuatu vatu',
        'WST': 'Samoan Tala',
        'XAF': 'Central African CFA franc',
        'XCD': 'East Caribbean Dollar',
        'XDR': 'Special Drawing Rights',
        'XOF': 'West African CFA franc',
        'XPF': 'French Pacific Franc',
        'YER': 'Yemeni Rial',
        'ZAR': 'South African Rand',
        'ZMK': 'Zambia Kwacha',
        'ZMW': 'Zambia Kwacha',

        # Commodity
        'XAU': 'Gold',
        'XAG': 'Silver',
    }

    __slots__ = ("base", "quote")

    def __init__(self, base: str, quote: str):
        self.base = base.upper()
        self.quote = quote.upper()
    
        self._validate()  # assert that base and quote are correct

    def _validate(self):
        if self.base not in ForexSymbol.CURRENCIES:
            raise ValueError(f"Invalid ForexSymbol's base currency: {self.base}")
        if self.quote not in ForexSymbol.CURRENCIES:
            raise ValueError(f"Invalid ForexSymbol's quote currency: {self.quote}")

    def __repr__(self):
        return f"ForexSymbol({self.base}, {self.quote})"

    def __str__(self):
        return f"{self.base}{self.quote}"
    
    def __eq__(self, other):
        return (
            isinstance(other, ForexSymbol)
            and self.base == other.base
            and self.quote == other.quote
        )

    def __hash__(self):
        return hash((self.base, self.quote))


class Timeframe:
    """
    Supported timeframes:
    - 1m, 5m, 15m, 30m
    - 1h, 4h
    - 1d
    - 1w
    - 1M
    """
    SECOND = "sec"
    MINUTE = "min"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"

    _UNIT_TO_PANDAS = {
        SECOND: "s",
        MINUTE: "min",
        HOUR: "h",
        DAY: "d",
        WEEK: "w",
        MONTH: "M",
    }

    def __init__(self, length: int, unit: str | None = None):
        self.length = length
        self.unit = unit

        self._validate_length_and_unit()

    def _validate_length_and_unit(self):
        if not (
            (self.unit == Timeframe.MINUTE and self.length in (1, 5, 15, 30))
            or (self.unit == Timeframe.HOUR and self.length in (1, 4))
            or (self.unit == Timeframe.DAY and self.length in (1,))
            or (self.unit == Timeframe.WEEK and self.length in (1,))
            or (self.unit == Timeframe.MONTH and self.length in (1,))
        ):
            raise ValueError(f"Timeframe not supported: '{self}'")
        
        if not isinstance(self.length, int):
            raise ValueError(f"Invalid Timeframe length: '{self}'")
    
    @property
    def timedelta(self) -> pd.Timedelta:
        if self.unit not in self._UNIT_TO_PANDAS:
            raise ValueError(f"Cannot convert unit to Timedelta: {self.unit}")

        if self.unit == Timeframe.MONTH:
            return pd.Timedelta(days=31)  # months got irregular days, use 31 days for all months
        else:
            return pd.Timedelta(self.length, unit=self._UNIT_TO_PANDAS[self.unit])

    @property
    def is_intraday(self) -> bool:
        return self.unit in (Timeframe.SECOND, Timeframe.MINUTE, Timeframe.HOUR)

    def __repr__(self):
        return f"Timeframe({self.length}, {self.unit})"

    def __str__(self):
        return f"{self.length}{self.unit}"
