{% set fred_features = {
    "gdp": "GDP",
    "us_30_year_mortgage_rate": "US 30 Year Mortgage Rate",
    "commercial_paper_outstanding": "Commercial Paper Outstanding",
    "us_oecd_indicator": "US OECD Indicator",
    "m_two": "M2",
    "producer_price_index": "Producer Price Index",
    "us_treasury_general_account": "US Treasury General Account",
    "cpi": "CPI",
    "unemployment_rate": "Unemployment Rate",
    "initial_claims": "Initial Claims",
    "non_farm_payrolls": "Non Farm Payrolls",
    "uni_michigan_consumer_sentiment": "Uni Michigan Consumer Sentiment",
    "industrial_production": "Industrial Production",
    "retail_sales": "Retail Sales",
    "personal_consumption_expenditures": "Personal Consumption Expenditures",
    "durable_goods_orders": "Durable Goods Orders",
    "current_account_balance": "Current Account Balance",
    "total_consumer_credit": "Total Consumer Credit",
    "bank_prime_loan_rate": "Bank Prime Loan Rate",
    "building_permits": "Building Permits",
    "capacity_utilization": "Capacity Utilization",
    "daily_vix": "Daily VIX",
    "gold_vix": "Gold VIX",
    "ten_year_inflation_rate": "10 Year Inflation Rate",
    "fed_funds_rate": "Fed Funds Rate",
    "us_emerging_markets_liquidity": "US Emerging Markets Liquidity",
    "oil_wti": "Oil WTI",
    "oil_brent": "Oil Brent",
    "fed_financial_conditions_index": "Fed Financial Conditions Index"
} %}

select
    date + interval '1 day' as date
    {% for feature, symbol in fred_features.items() %}
    , first_value({{ feature }}) over (partition by {{ feature }}_count order by date) as {{ feature }}
    {% endfor %}
from (
	select
		*
		{% for feature, symbol in fred_features.items() %}
		, sum(case when {{ feature }} is not null then 1 end) over (order by date) as {{ feature }}_count
		{% endfor %}
	from {{ source('public', 'fed_features') }}
	order by date) fred