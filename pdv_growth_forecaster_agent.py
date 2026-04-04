import subprocess
import argparse
import pandas as pd
import numpy as np
try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:
    print("[Warning] sklearn not installed. Falling back to simple moving averages for forecasting.")
    SKLEARN_AVAILABLE = False

class PDVForecasterAgent:
    """
    Subagent: Forecasting & Predictive Analyst.
    Scope: Take historical data (demographics, health facilities, real estate permits)
    and forecast future growth to evaluate a region's attractiveness for new PDVs.
    """
    def __init__(self, wsl_command="hermes"):
        self.wsl_command = wsl_command
        
    def think_with_hermes(self, prompt: str) -> str:
        """Consult local Hermes via WSL to synthesize and score the forecast."""
        full_command = ["wsl", self.wsl_command, prompt]
        print(f"\n[Forecast Agent] Synthesizing insights with '{self.wsl_command}'...")
        try:
            result = subprocess.run(
                full_command, capture_output=True, text=True, check=True, encoding='utf-8'
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            return f"Error executing {self.wsl_command}: {e.stderr}"
        except FileNotFoundError:
            return "WSL not found."

    def forecast_variable(self, df_history, steps=3):
        """
        Forecasting function using Linear Regression over time.
        df_history should be a DataFrame with ['year', 'value']
        Returns a DataFrame with the original and forecasted future values.
        """
        if df_history.empty:
            return None
            
        future_years = np.array([df_history['year'].max() + i for i in range(1, steps + 1)])
        
        if SKLEARN_AVAILABLE:
            model = LinearRegression()
            X = df_history['year'].values.reshape(-1, 1)
            y = df_history['value'].values
            model.fit(X, y)
            predictions = model.predict(future_years.reshape(-1, 1))
        else: # Simple naive baseline average
            avg_growth = df_history['value'].diff().mean()
            last_val = df_history['value'].iloc[-1]
            predictions = [max(0, last_val + (avg_growth * i)) for i in range(1, steps + 1)]

        df_future = pd.DataFrame({'year': future_years, 'forecast': predictions})
        return df_future

    def analyze_region(self, bairro_name: str, historical_data_mock: dict):
        """
        Takes historical vectors for different factors of a Bairro and
        returns a synthesized evaluation for placing a PDV (Farmácia).
        """
        print(f"=========================================================")
        print(f"  PDV FORECASTER AGENT: Analyzing {bairro_name.upper()}")
        print(f"=========================================================")

        analysis_context = f"Region: {bairro_name}\n\n"
        
        for variable, hist_df in historical_data_mock.items():
            print(f"> Forecasting [{variable}] for the next 3 years...")
            
            # Run the forecast
            forecast_df = self.forecast_variable(hist_df, steps=3)
            
            # Combine history and forecast into text context
            hist_str = ", ".join([f"{row.year}:{row.value:.1f}" for _, row in hist_df.iterrows()])
            fut_str = ", ".join([f"{row.year}:{row.forecast:.1f}" for _, row in forecast_df.iterrows()])
            
            analysis_context += f"--- {variable.upper()} ---\n"
            analysis_context += f"Historical (Past 5 years): {hist_str}\n"
            analysis_context += f"Forecast (Next 3 years): {fut_str}\n\n"

        prompt = f"""
You are an Expert Real Estate and Market Forecaster.
I am providing you with the 5-year historical data AND a 3-year computational forecast for the neighborhood '{bairro_name}'.

The variables are:
1. Aging Rate (Taxa de envelhecimento - % of population over 60)
2. Real Estate Permits (New residential and logistical center constructions per year)
3. Existing Pharmacies (Current market saturation)

DATA:
{analysis_context}

Based on this trend data:
1. Identify if this neighborhood is cooling down, stagnant, or gentrifying/growing.
2. Given the aging population trend and the construction boom, is this an optimal location for a NEW Pharmacy PDV?
3. Provide a final 'Attractiveness Score' from 1 to 10.
4. Keep the evaluation concise and strictly actionable.
        """
        
        synthesis = self.think_with_hermes(prompt)
        print("\n--- HERMES MARKET FORECAST & PDV SCORING ---")
        print(synthesis)
        print("------------------------------------------\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--command", default="hermes", help="Command to run Hermes in WSL")
    args = parser.parse_args()
    
    agent = PDVForecasterAgent(wsl_command=args.command)
    
    # Mock data simulating what the Researcher Agent extracted
    bairro_example = "Vila Olimpia"
    
    # Aging rate (%)
    df_aging = pd.DataFrame({'year': [2018, 2019, 2020, 2021, 2022], 'value': [15.2, 15.8, 16.5, 17.1, 18.0]})
    # New real estate permits (houses, residential buildings, logistics)
    df_permits = pd.DataFrame({'year': [2018, 2019, 2020, 2021, 2022], 'value': [120, 145, 90, 210, 315]})
    # Number of Pharmacies
    df_pharmas = pd.DataFrame({'year': [2018, 2019, 2020, 2021, 2022], 'value': [14, 15, 15, 16, 17]})
    
    mock_data = {
        "Taxa de Envelhecimento (%)": df_aging,
        "Alvarás de Construção (Qtd)": df_permits,
        "Farmácias Existentes (Qtd)": df_pharmas
    }
    
    agent.analyze_region(bairro_example, mock_data)
