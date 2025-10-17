{{ 
  dbt_date.get_date_dimension(
      "2025-01-01", 
      modules.datetime.datetime.now().replace(month=12, day=31).strftime('%Y-%m-%d')
  ) 
}}