def model(dbt, fal):

    import numpy as np
    import pandas as pd
    from scipy.stats import gaussian_kde
    from statsmodels.tsa.statespace.kalman_smoother import KalmanSmoother

    np.random.seed(123)

    Q = float(dbt.config.get("Q"))
    H = float(dbt.config.get("H"))
    skew_threshold = float(dbt.config.get("skew_threshold"))
    bitcoin_dominance_threshold = float(dbt.config.get("bitcoin_dominance_threshold"))
    kfs_lambda = float(dbt.config.get("kfs_lambda"))
    kfs_distribution_shift = float(dbt.config.get("kfs_distribution_shift"))
    kfs_window_size = int(dbt.config.get("kfs_window_size"))

    df: pd.DataFrame = dbt.ref("sm_feature_matrix")
    df = df[["date_index", "log_return", "skew_estimate", "bitcoin_dominance"]]

    df['lead_log_return'] = df['log_return'].shift(-1)
    df = df.dropna()

    y = df['skew_estimate'] + 1
    y = np.log(y) - np.log(y.shift(1))
    df['skew_log_return'] = y
    df = df.dropna()

    model = KalmanSmoother(
        k_endog=1,
        k_states=1,
        transition=[[1]],
        selection=[[1]],
        state_intercept=[0],
        design=[[1]],
        obs_intercept=[0],
        obs_cov=[[H]],
        state_cov=[[Q]],
    )

    model.bind(df['skew_log_return'].values)
    model.initialize_known([0], [[0]])

    results = model.filter()
    forecast_cumsum = np.cumsum(results.forecasts)

    min_val = forecast_cumsum.min()
    max_val = forecast_cumsum.max()

    scaled_data = (forecast_cumsum - min_val) / (max_val - min_val)

    df['kalman_filter'] = scaled_data
    df['indicator'] = np.where((df['kalman_filter'] < skew_threshold) &
                               (df['bitcoin_dominance'] > bitcoin_dominance_threshold), -1, 1)
    df['adj_log_return'] = df['indicator'] * df['lead_log_return']

    df['adj_log_return'] = df['indicator'] * df['lead_log_return']

    window_size = kfs_window_size
    weights = []

    for i in range(len(df) - window_size + 1):
        kde = gaussian_kde(df['adj_log_return'].iloc[i:i + window_size])
        x_grid = np.linspace(min(df['adj_log_return'].iloc[i:i + window_size]),
                             max(df['adj_log_return'].iloc[i:i + window_size]), 512)
        pdf_values = kde(x_grid)
        pdf_values = pdf_values / pdf_values.sum()

        lambda_value = kfs_lambda
        transformed_returns = ((df['adj_log_return'].iloc[i:i + window_size] + 1) ** lambda_value - 1) / lambda_value

        kde = gaussian_kde(transformed_returns)
        transformed_x_grid = np.linspace(min(transformed_returns), max(transformed_returns), 512)
        transformed_pdf_values = kde(transformed_x_grid)
        transformed_pdf_values = transformed_pdf_values / transformed_pdf_values.sum()

        # Shift the density estimate to match the peak of the original density
        transformed_x_grid = transformed_x_grid + kfs_distribution_shift * (
                    x_grid[np.argmax(pdf_values)] - transformed_x_grid[np.argmax(transformed_pdf_values)])

        weight = np.interp(transformed_returns, transformed_x_grid, transformed_pdf_values)
        weight = weight / weight.max()

        if i == 0:
            weights = weights + list(weight)
        else:
            weights.append(weight[-1])

    df['weight'] = weights

    df.drop(columns=['skew_log_return'], inplace=True)

    return df
