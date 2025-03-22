
import runpod
import requests
import json
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

def map_equipment_code_to_rateview(equipment_code: str) -> str:
    """Maps load search API equipment codes to Rateview API equipment types."""
    van_codes = ["V", "VA", "VB", "VC", "V2", "VZ", "VH", "VI", "VN", "VG", "VL", "VV", "VM", "VT", "VF", "VR", "VP", "VW"]
    reefer_codes = ["R", "RA", "R2", "RZ", "RN", "RL", "RM", "RG", "RV", "RP"]
    flatbed_codes = ["F", "FA", "FT", "FM", "FD", "FR", "FO", "FN", "FS"]
    
    if equipment_code in van_codes:
        return "VAN"
    elif equipment_code in reefer_codes:
        return "REEFER"
    elif equipment_code in flatbed_codes:
        return "FLATBED"
    else:
        # Default to FLATBED if unknown
        return "FLATBED"

# ==== WEATHER FUNCTIONS ====

def get_weather_description(code):
    """Convert WMO weather code to human-readable description."""
    weather_codes = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Depositing rime fog",
        51: "Light drizzle",
        53: "Moderate drizzle",
        55: "Dense drizzle",
        56: "Light freezing drizzle",
        57: "Dense freezing drizzle",
        61: "Slight rain",
        63: "Moderate rain",
        65: "Heavy rain",
        66: "Light freezing rain",
        67: "Heavy freezing rain",
        71: "Slight snow fall",
        73: "Moderate snow fall",
        75: "Heavy snow fall",
        77: "Snow grains",
        80: "Slight rain showers",
        81: "Moderate rain showers",
        82: "Violent rain showers",
        85: "Slight snow showers",
        86: "Heavy snow showers",
        95: "Thunderstorm",
        96: "Thunderstorm with slight hail",
        99: "Thunderstorm with heavy hail"
    }
    
    return weather_codes.get(code, f"Unknown weather code: {code}")

def get_weather_data(lat, lon, forecast_days=2):
    """Get weather forecast data for a specific location using Open-Meteo API."""
    api_url = "https://api.open-meteo.com/v1/forecast"
    
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,windspeed_10m,weathercode",
        "daily": "weathercode,temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "auto",
        "forecast_days": forecast_days
    }
    
    try:
        response = requests.get(api_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            result = {
                "location": {
                    "latitude": data["latitude"],
                    "longitude": data["longitude"],
                    "timezone": data["timezone"],
                    "elevation": data.get("elevation")
                },
                "hourly_forecast": [],
                "daily_forecast": [],
                "api_status": "success"
            }
            
            # Process hourly forecast
            hourly_time = data["hourly"]["time"]
            hourly_temp = data["hourly"]["temperature_2m"]
            hourly_precip = data["hourly"]["precipitation"]
            hourly_wind = data["hourly"]["windspeed_10m"]
            hourly_weather = data["hourly"]["weathercode"]
            
            for i in range(min(24, len(hourly_time))):  # Limit to first 24 hours
                result["hourly_forecast"].append({
                    "time": hourly_time[i],
                    "temperature_celsius": hourly_temp[i],
                    "precipitation_mm": hourly_precip[i],
                    "wind_speed_kmh": hourly_wind[i],
                    "weather_code": hourly_weather[i],
                    "weather_description": get_weather_description(hourly_weather[i])
                })
            
            # Process daily forecast
            daily_time = data["daily"]["time"]
            daily_weather = data["daily"]["weathercode"]
            daily_temp_max = data["daily"]["temperature_2m_max"]
            daily_temp_min = data["daily"]["temperature_2m_min"]
            daily_precip = data["daily"]["precipitation_sum"]
            
            for i in range(len(daily_time)):
                result["daily_forecast"].append({
                    "date": daily_time[i],
                    "weather_code": daily_weather[i],
                    "weather_description": get_weather_description(daily_weather[i]),
                    "max_temperature_celsius": daily_temp_max[i],
                    "min_temperature_celsius": daily_temp_min[i],
                    "precipitation_sum_mm": daily_precip[i]
                })
            
            return result
        else:
            return {
                "error": f"API error: {response.status_code}",
                "api_status": "error"
            }
    
    except Exception as e:
        return {
            "error": f"Exception: {str(e)}",
            "api_status": "exception"
        }

def check_for_hazards(weather_data):
    """Analyze weather data to identify potential hazards."""
    if "error" in weather_data:
        return {"error": weather_data["error"]}
    
    hazards = []
    
    # Check hourly forecast for hazards
    for hour in weather_data["hourly_forecast"]:
        # Check for severe weather codes
        weather_code = hour["weather_code"]
        if weather_code in [71, 73, 75, 77, 85, 86]:  # Snow
            hazards.append({
                "type": "Snow",
                "time": hour["time"],
                "severity": "high" if weather_code in [75, 86] else "medium"
            })
        elif weather_code in [95, 96, 99]:  # Thunderstorms
            hazards.append({
                "type": "Thunderstorm",
                "time": hour["time"],
                "severity": "high"
            })
        elif weather_code in [66, 67]:  # Freezing rain
            hazards.append({
                "type": "Freezing Rain",
                "time": hour["time"],
                "severity": "high"
            })
        elif weather_code in [45, 48]:  # Fog
            hazards.append({
                "type": "Fog",
                "time": hour["time"],
                "severity": "medium"
            })
        
        # Check for heavy precipitation
        if hour["precipitation_mm"] > 4:
            hazards.append({
                "type": "Heavy Rain",
                "time": hour["time"],
                "precipitation": hour["precipitation_mm"],
                "severity": "medium"
            })
        
        # Check for high winds
        if hour["wind_speed_kmh"] > 30:
            hazards.append({
                "type": "High Winds",
                "time": hour["time"],
                "wind_speed": hour["wind_speed_kmh"],
                "severity": "medium" if hour["wind_speed_kmh"] < 50 else "high"
            })
    
    return {
        "location": weather_data["location"],
        "hazards": hazards,
        "hazard_count": len(hazards),
        "has_severe_hazards": any(h["severity"] == "high" for h in hazards)
    }

def estimate_weather_delay(hazards, trip_miles):
    """
    Estimate the delay in hours that a driver might experience due to weather conditions.
    
    Args:
        hazards: List of weather hazards
        trip_miles: Total trip distance in miles
        
    Returns:
        Dictionary with delay estimates
    """
    if not hazards or trip_miles <= 0:
        return {
            "estimated_delay_hours": 0,
            "delay_factors": [],
            "impact_level": "None"
        }
    
    # Define speed reduction factors for different weather conditions
    # Format: (percentage of normal speed, percentage of route affected)
    weather_impacts = {
        "Snow": {
            "high": (0.4, 0.6),  # 40% of normal speed for 60% of route
            "medium": (0.6, 0.5)  # 60% of normal speed for 50% of route
        },
        "Freezing Rain": {
            "high": (0.35, 0.5),
            "medium": (0.5, 0.4)
        },
        "Thunderstorm": {
            "high": (0.5, 0.3),
            "medium": (0.7, 0.2)
        },
        "Fog": {
            "high": (0.4, 0.4),
            "medium": (0.6, 0.3)
        },
        "Heavy Rain": {
            "high": (0.6, 0.4),
            "medium": (0.7, 0.3)
        },
        "High Winds": {
            "high": (0.7, 0.6),
            "medium": (0.8, 0.4)
        }
    }
    
    # Assume average speed of 55 mph under normal conditions
    normal_speed_mph = 55
    
    # Calculate normal trip time in hours
    normal_trip_time = trip_miles / normal_speed_mph
    
    # Track total delay and factors
    total_delay_hours = 0
    delay_factors = []
    
    # Process each weather hazard
    for hazard in hazards:
        hazard_type = hazard.get("type")
        severity = hazard.get("severity", "medium")
        
        if hazard_type in weather_impacts and severity in weather_impacts[hazard_type]:
            # Get impact parameters for this hazard
            speed_factor, route_affected = weather_impacts[hazard_type][severity]
            
            # Calculate affected miles
            affected_miles = trip_miles * route_affected
            
            # Calculate normal time for affected segment
            normal_segment_time = affected_miles / normal_speed_mph
            
            # Calculate actual time for affected segment with reduced speed
            actual_segment_time = affected_miles / (normal_speed_mph * speed_factor)
            
            # Calculate delay for this segment
            segment_delay = actual_segment_time - normal_segment_time
            
            # Only count significant delays (more than 15 minutes)
            if segment_delay > 0.25:
                total_delay_hours += segment_delay
                delay_factors.append({
                    "type": hazard_type,
                    "severity": severity,
                    "delay_hours": round(segment_delay, 1)
                })
    
    # Determine impact level
    if total_delay_hours < 0.5:
        impact_level = "Minimal"
    elif total_delay_hours < 2:
        impact_level = "Moderate"
    elif total_delay_hours < 4:
        impact_level = "Significant"
    else:
        impact_level = "Severe"
    
    return {
        "estimated_delay_hours": round(total_delay_hours, 1),
        "delay_percentage": round((total_delay_hours / normal_trip_time) * 100, 1),
        "delay_factors": delay_factors,
        "impact_level": impact_level
    }

def get_simple_weather_analysis(origin_weather_data, destination_weather_data, trip_miles):
    """Create a simple weather analysis summary for drivers."""
    
    analysis = {
        "summary": "No weather data available",
        "risk_level": "Unknown",
        "risk_score": 0,
        "hazards": [],
        "estimated_delay": {
            "estimated_delay_hours": 0,
            "delay_factors": [],
            "impact_level": "None"
        }
    }
    
    # Check if we have valid weather data
    if not origin_weather_data or not destination_weather_data:
        return analysis
    
    origin_hazards = None
    dest_hazards = None
    all_hazards = []
    
    # Process origin weather data
    if "error" not in origin_weather_data and origin_weather_data.get("api_status") == "success":
        origin_hazards = check_for_hazards(origin_weather_data)
        if "error" not in origin_hazards:
            for hazard in origin_hazards.get("hazards", []):
                analysis["hazards"].append(f"Origin: {hazard['type']} ({hazard['severity']} risk)")
                all_hazards.append(hazard)
    
    # Process destination weather data
    if "error" not in destination_weather_data and destination_weather_data.get("api_status") == "success":
        dest_hazards = check_for_hazards(destination_weather_data)
        if "error" not in dest_hazards:
            for hazard in dest_hazards.get("hazards", []):
                analysis["hazards"].append(f"Destination: {hazard['type']} ({hazard['severity']} risk)")
                all_hazards.append(hazard)
    
    # Get first day forecast summaries where available
    origin_weather = "Unknown"
    destination_weather = "Unknown"
    
    if origin_weather_data and "daily_forecast" in origin_weather_data and origin_weather_data["daily_forecast"]:
        first_day = origin_weather_data["daily_forecast"][0]
        origin_weather = first_day.get("weather_description", "Unknown")
    
    if destination_weather_data and "daily_forecast" in destination_weather_data and destination_weather_data["daily_forecast"]:
        first_day = destination_weather_data["daily_forecast"][0]
        destination_weather = first_day.get("weather_description", "Unknown")
    
    # Calculate a risk score from hazards
    risk_score = 0
    if origin_hazards and "hazards" in origin_hazards:
        severe_hazards = [h for h in origin_hazards["hazards"] if h["severity"] == "high"]
        medium_hazards = [h for h in origin_hazards["hazards"] if h["severity"] == "medium"]
        risk_score += len(severe_hazards) * 20 + len(medium_hazards) * 10
    
    if dest_hazards and "hazards" in dest_hazards:
        severe_hazards = [h for h in dest_hazards["hazards"] if h["severity"] == "high"]
        medium_hazards = [h for h in dest_hazards["hazards"] if h["severity"] == "medium"]
        risk_score += len(severe_hazards) * 15 + len(medium_hazards) * 5
    
    # Cap risk score at 100
    risk_score = min(risk_score, 100)
    
    # Determine risk level
    if risk_score == 0:
        risk_level = "None"
    elif risk_score < 25:
        risk_level = "Low"
    elif risk_score < 50:
        risk_level = "Medium"
    elif risk_score < 75:
        risk_level = "High"
    else:
        risk_level = "Severe"
    
    # Estimate delay due to weather conditions
    delay_info = estimate_weather_delay(all_hazards, trip_miles)
    
    # Create summary
    if analysis["hazards"]:
        hazard_types = set()
        for hazard in analysis["hazards"]:
            for h_type in ["Snow", "Thunderstorm", "Freezing Rain", "Fog", "Heavy Rain", "High Winds"]:
                if h_type in hazard:
                    hazard_types.add(h_type)
        
        hazard_list = ", ".join(sorted(list(hazard_types)))
        
        # Add delay information to summary
        if delay_info["estimated_delay_hours"] > 0:
            delay_hours = delay_info["estimated_delay_hours"]
            impact = delay_info["impact_level"]
            analysis["summary"] = f"Weather conditions include {hazard_list}. Expect {impact.lower()} delays of approximately {delay_hours} hours. Origin: {origin_weather}, Destination: {destination_weather}"
        else:
            analysis["summary"] = f"Weather conditions include {hazard_list}. No significant delays expected. Origin: {origin_weather}, Destination: {destination_weather}"
    else:
        analysis["summary"] = f"No significant weather hazards. Origin: {origin_weather}, Destination: {destination_weather}"
    
    analysis["risk_level"] = risk_level
    analysis["risk_score"] = risk_score
    analysis["estimated_delay"] = delay_info
    
    return analysis

# NEW FUNCTION: DEADHEAD ANALYSIS 
def calculate_deadhead_analysis(origin_deadhead_miles, destination_deadhead_miles, trip_miles):
    """
    Provide detailed analysis of deadhead miles impact on profitability.
    
    Args:
        origin_deadhead_miles: Miles from current position to pickup location
        destination_deadhead_miles: Miles from delivery to next likely pickup
        trip_miles: Total loaded trip distance
        
    Returns:
        Dictionary with deadhead analysis
    """
    if trip_miles <= 0:
        return {
            "status": "invalid",
            "message": "Trip miles must be greater than zero"
        }
    
    # Calculate total deadhead miles
    total_deadhead = origin_deadhead_miles + destination_deadhead_miles
    
    # Calculate deadhead ratios
    origin_deadhead_ratio = round(origin_deadhead_miles / trip_miles, 2) if trip_miles > 0 else 0
    destination_deadhead_ratio = round(destination_deadhead_miles / trip_miles, 2) if trip_miles > 0 else 0
    total_deadhead_ratio = round(total_deadhead / trip_miles, 2) if trip_miles > 0 else 0
    
    # Calculate paid vs. unpaid miles ratio
    total_distance = trip_miles + total_deadhead
    paid_miles_percentage = round((trip_miles / total_distance) * 100, 1) if total_distance > 0 else 0
    unpaid_miles_percentage = round((total_deadhead / total_distance) * 100, 1) if total_distance > 0 else 0
    
    # Determine deadhead severity
    if total_deadhead_ratio <= 0.1:
        severity = "Excellent"
        impact = "Very Low"
    elif total_deadhead_ratio <= 0.2:
        severity = "Good"
        impact = "Low"
    elif total_deadhead_ratio <= 0.3:
        severity = "Average"
        impact = "Moderate"
    elif total_deadhead_ratio <= 0.5:
        severity = "Poor"
        impact = "High"
    else:
        severity = "Very Poor"
        impact = "Severe"
    
    # Generate summary
    if origin_deadhead_miles > destination_deadhead_miles:
        focus = "Pickup deadhead is your main concern."
    elif destination_deadhead_miles > origin_deadhead_miles:
        focus = "Consider your next load after delivery."
    else:
        focus = ""
    
    summary = f"Deadhead is {severity.lower()} at {round(total_deadhead_ratio * 100)}% of loaded miles. " + focus
    
    return {
        "status": "success",
        "origin_deadhead_miles": origin_deadhead_miles,
        "destination_deadhead_miles": destination_deadhead_miles,
        "total_deadhead_miles": total_deadhead,
        "origin_deadhead_ratio": origin_deadhead_ratio,
        "destination_deadhead_ratio": destination_deadhead_ratio,
        "total_deadhead_ratio": total_deadhead_ratio,
        "paid_miles_percentage": paid_miles_percentage,
        "unpaid_miles_percentage": unpaid_miles_percentage,
        "severity": severity,
        "impact": impact,
        "summary": summary
    }

def calculate_load_score(rate_comparison, weather_risk_score, deadhead_ratio, trip_miles, driver_pay, weather_delay_hours):
    """Calculate an overall load quality score (0-100)."""
    # Start with a base score of 50
    quality_score = 50
    factors = []
    
    # Factor 1: Rate comparison (up to +/- 25 points)
    if isinstance(rate_comparison, (int, float)):
        # Cap at +/- 30% for scoring purposes
        capped_difference = max(min(rate_comparison, 30), -30)
        # Convert to score component (-25 to +25)
        rate_score = capped_difference * (25/30)
        quality_score += rate_score
        
        if rate_comparison > 0:
            factors.append(f"{round(rate_comparison, 1)}% above market rate")
        elif rate_comparison < 0:
            factors.append(f"{abs(round(rate_comparison, 1))}% below market rate")
    
    # Factor 2: Weather risk (up to -15 points)
    weather_impact = -(weather_risk_score / 6.67)  # Scale 0-100 to 0-15
    quality_score += weather_impact
    
    if weather_risk_score > 50:
        factors.append("High weather risk")
    elif weather_risk_score > 25:
        factors.append("Moderate weather risk")
    
    # Factor 3: Weather delay impact (up to -15 points)
    if weather_delay_hours > 0:
        # Max penalty is 15 points for 5+ hour delay
        delay_impact = -min(weather_delay_hours * 3, 15)
        quality_score += delay_impact
        
        if weather_delay_hours >= 4:
            factors.append(f"Severe delay risk ({weather_delay_hours} hrs)")
        elif weather_delay_hours >= 2:
            factors.append(f"Significant delay risk ({weather_delay_hours} hrs)")
    
    # Factor 4: Deadhead impact (up to -20 points) - ENHANCED
    if deadhead_ratio > 0:
        # Scale from 0 to 0.5 (or higher) to 0 to -20 points
        deadhead_impact = -min(deadhead_ratio * 40, 20)
        quality_score += deadhead_impact
        
        if deadhead_ratio > 0.4:
            factors.append(f"Excessive deadhead ({round(deadhead_ratio * 100)}%)")
        elif deadhead_ratio > 0.25:
            factors.append(f"High deadhead ({round(deadhead_ratio * 100)}%)")
    
    # Factor 5: Driver pay (up to +15 points)
    if isinstance(driver_pay, (int, float)) and driver_pay > 0:
        # Simple tier-based bonus
        if driver_pay > 500:
            pay_bonus = 15
            factors.append("Excellent pay")
        elif driver_pay > 350:
            pay_bonus = 10
            factors.append("Good pay")
        elif driver_pay > 250:
            pay_bonus = 5
        else:
            pay_bonus = 0
            
        quality_score += pay_bonus
    
    # Ensure score is between 0 and 100
    final_score = round(max(0, min(100, quality_score)))
    
    # Determine quality category
    if final_score >= 80:
        category = "Excellent"
    elif final_score >= 65:
        category = "Good"
    elif final_score >= 45:
        category = "Average"
    elif final_score >= 25:
        category = "Poor"
    else:
        category = "Very Poor"
    
    return {
        "score": final_score,
        "category": category,
        "key_factors": factors[:3]  # Limit to top 3 factors
    }

# ==== ORIGINAL FREIGHT RATE FUNCTIONS ====

def get_broker_rate_per_mile(load: Dict[str, Any]) -> Optional[float]:
    """Extract broker rate per mile from load data."""
    # Try estimated rate per mile first
    if load.get("estimatedRatePerMile", 0) > 0:
        return load["estimatedRatePerMile"]
    
    # Try to calculate from total rate and trip length
    trip_length_miles = load.get("tripLength", {}).get("miles", 0)
    if trip_length_miles <= 0:
        return None
    
    # Check for privateNetworkRateInfo
    private_rate = load.get("privateNetworkRateInfo", {}).get("bookable", {}).get("rate", {}).get("rateUsd", 0)
    if private_rate > 0:
        return private_rate / trip_length_miles
    
    # Check for loadBoardRateInfo
    load_board_rate = load.get("loadBoardRateInfo", {}).get("nonBookable", {}).get("rateUsd", 0)
    if load_board_rate > 0:
        return load_board_rate / trip_length_miles
    
    return None

def get_total_load_amount(load: Dict[str, Any]) -> Optional[float]:
    """Extract total load amount from load data."""
    # Check for privateNetworkRateInfo
    private_rate = load.get("privateNetworkRateInfo", {}).get("bookable", {}).get("rate", {}).get("rateUsd", 0)
    if private_rate > 0:
        return private_rate
    
    # Check for loadBoardRateInfo
    load_board_rate = load.get("loadBoardRateInfo", {}).get("nonBookable", {}).get("rateUsd", 0)
    if load_board_rate > 0:
        return load_board_rate
    
    return None

def calculate_driver_pay(load: Dict[str, Any]) -> Dict[str, Union[float, str]]:
    """
    Calculate driver pay as 25% of total load amount. 
    If load amount not available, calculate as rate per mile * trip length.
    """
    trip_length_miles = load.get("tripLength", {}).get("miles", 0)
    
    # Try to get total load amount
    total_load_amount = get_total_load_amount(load)
    
    if total_load_amount and total_load_amount > 0:
        # Driver pay is 25% of total load amount
        driver_pay = total_load_amount * 0.25
        source = "percentage_of_total"
    else:
        # Get rate per mile
        rate_per_mile = get_broker_rate_per_mile(load)
        
        if rate_per_mile and rate_per_mile > 0 and trip_length_miles > 0:
            # Calculate total from rate per mile and trip length
            total_calculated = rate_per_mile * trip_length_miles
            driver_pay = total_calculated * 0.25
            source = "calculated_from_rate_per_mile"
        else:
            # Not enough data to calculate
            return {
                "amount": "Not Available",
                "calculation_method": "insufficient_data"
            }
    
    return {
        "amount": round(driver_pay, 2),
        "calculation_method": source
    }

def get_rate_comparison(load_rate: Optional[float], market_rate: Optional[float]) -> Dict[str, Union[float, str]]:
    """Calculate percentage difference between broker rate and market rate."""
    if load_rate is None or load_rate <= 0 or market_rate is None or market_rate <= 0:
        return {
            "broker_rate_per_mile": "Not Available" if load_rate is None or load_rate <= 0 else load_rate,
            "market_rate_per_mile": "Not Available" if market_rate is None or market_rate <= 0 else market_rate,
            "difference_percentage": "N/A",
            "comparison": "Rate comparison not possible"
        }
    
    difference_percentage = ((load_rate - market_rate) / market_rate) * 100
    
    comparison = (
        f"{abs(round(difference_percentage, 2))}% above market rate"
        if difference_percentage > 0
        else f"{abs(round(difference_percentage, 2))}% below market rate"
        if difference_percentage < 0
        else "At market rate"
    )
    
    return {
        "broker_rate_per_mile": load_rate,
        "market_rate_per_mile": market_rate,
        "difference_percentage": round(difference_percentage, 2),
        "comparison": comparison
    }

def call_rateview_api(origin: Dict[str, str], destination: Dict[str, str], 
                     equipment: str, access_token: str) -> Dict[str, Any]:
    """Call the Rateview API to get market rate information."""
    base_url = "https://analytics.api.staging.dat.com/linehaulrates"
    endpoint = "/v1/lookups"
    url = base_url + endpoint
    
    # Format the payload
    payload = [{
        "origin": {
            "city": origin.get("city", ""),
            "stateOrProvince": origin.get("stateProv", "")
        },
        "destination": {
            "city": destination.get("city", ""),
            "stateOrProvince": destination.get("stateProv", "")
        },
        "rateType": "SPOT",
        "equipment": equipment,
        "includeMyRate": True,
        "targetEscalation": {
            "escalationType": "BEST_FIT"
        }
    }]
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code in (200, 201):
            return response.json()
        else:
            return {"error": f"API Error: {response.status_code}", "detail": response.text}
    except Exception as e:
        return {"error": f"Exception: {str(e)}"}

def process_loads_and_compare_rates(loads_data: Dict[str, Any], access_token: str) -> Dict[str, Any]:
    """
    Process loads data, call Rateview API, and calculate rate comparisons.
    Now includes weather data, delay estimates, and deadhead analysis.
    """
    result = {
        "matchCounts": loads_data.get("matchCounts", {}),
        "processedMatches": []
    }
    
    matches = loads_data.get("matches", [])
    
    for match in matches:
        match_id = match.get("matchId", "")
        
        # Extract origin and destination
        matching_asset_info = match.get("matchingAssetInfo", {})
        origin = matching_asset_info.get("origin", {})
        destination_place = matching_asset_info.get("destination", {}).get("place", {})
        
        # Skip if missing critical data
        if not origin or not destination_place:
            continue
        
        # Extract equipment type and map to Rateview format
        equipment_code = matching_asset_info.get("equipmentType", "")
        rateview_equipment = map_equipment_code_to_rateview(equipment_code)
        
        # Get trip miles
        trip_miles = match.get("tripLength", {}).get("miles", 0)
        
        # Get broker rate per mile
        broker_rate_per_mile = get_broker_rate_per_mile(match)
        
        # Calculate driver pay
        driver_pay = calculate_driver_pay(match)
        
        # === Get weather data if coordinates are available
        origin_weather_data = None
        destination_weather_data = None
        
        if origin.get("latitude") and origin.get("longitude"):
            origin_weather_data = get_weather_data(origin["latitude"], origin["longitude"])
        
        if destination_place.get("latitude") and destination_place.get("longitude"):
            destination_weather_data = get_weather_data(destination_place["latitude"], destination_place["longitude"])
        
        # Get simplified weather analysis with delay estimation
        weather_analysis = get_simple_weather_analysis(origin_weather_data, destination_weather_data, trip_miles)
        weather_delay_hours = weather_analysis.get("estimated_delay", {}).get("estimated_delay_hours", 0)
        
        # === NEW: Get deadhead miles and calculate analysis
        origin_deadhead_miles = match.get("originDeadheadMiles", {}).get("miles", 0)
        destination_deadhead_miles = match.get("destinationDeadheadMiles", {}).get("miles", 0)
        
        # Calculate deadhead analysis
        deadhead_analysis = calculate_deadhead_analysis(
            origin_deadhead_miles,
            destination_deadhead_miles,
            trip_miles
        )
        
        # Call Rateview API
        rateview_response = call_rateview_api(origin, destination_place, rateview_equipment, access_token)
        
        # Extract market rate per mile
        market_rate_per_mile = None
        market_data = None
        rate_data = None
        
        # Process rateview response
        try:
            # Check for API errors
            if "error" in rateview_response:
                market_data = {"error": rateview_response.get("error")}
            else:
                # Get the first rate response
                rate_responses = rateview_response.get("rateResponses", [])
                if rate_responses and len(rate_responses) > 0:
                    response_obj = rate_responses[0].get("response", {})
                    
                    # Extract rate data
                    if "rate" in response_obj:
                        rate_data = response_obj["rate"]
                        
                        # Extract market rate per mile if available
                        if "perMile" in rate_data and "rateUsd" in rate_data["perMile"]:
                            market_rate_per_mile = rate_data["perMile"]["rateUsd"]
                    
                    # Store complete market data
                    market_data = response_obj
        except Exception as e:
            market_data = {"error": f"Error processing rate data: {str(e)}"}
        
        # Calculate rate comparison
        comparison = get_rate_comparison(broker_rate_per_mile, market_rate_per_mile)
        
        # Calculate load quality score with weather and deadhead factors
        load_score = calculate_load_score(
            comparison.get("difference_percentage") if comparison.get("difference_percentage") != "N/A" else 0,
            weather_analysis.get("risk_score", 0),
            deadhead_analysis.get("total_deadhead_ratio", 0),
            trip_miles,
            driver_pay.get("amount") if isinstance(driver_pay.get("amount"), (int, float)) else 0,
            weather_delay_hours
        )
        
        # Build processed match data - INCLUDE BOTH WEATHER AND DEADHEAD ANALYSIS
        processed_match = {
            "matchId": match_id,
            "origin": {
                "city": origin.get("city", ""),
                "state": origin.get("stateProv", "")
            },
            "destination": {
                "city": destination_place.get("city", ""),
                "state": destination_place.get("stateProv", "")
            },
            "equipmentType": {
                "code": equipment_code,
                "rateviewType": rateview_equipment
            },
            "tripMiles": trip_miles,
            "rateComparison": comparison,
            "driver_pay": driver_pay,
            
            # Weather info with delay estimates
            "weatherInfo": {
                "summary": weather_analysis.get("summary", "Weather data not available"),
                "risk_level": weather_analysis.get("risk_level", "Unknown"),
                "risk_score": weather_analysis.get("risk_score", 0),
                "hazards": weather_analysis.get("hazards", []),
                "estimated_delay": weather_analysis.get("estimated_delay", {
                    "estimated_delay_hours": 0,
                    "delay_factors": [],
                    "impact_level": "None"
                })
            },
            
            # Deadhead analysis
            "deadheadAnalysis": deadhead_analysis,
            
            # Load quality score (now factors in both weather and deadhead)
            "loadQuality": {
                "score": load_score.get("score", 0),
                "category": load_score.get("category", "Unknown"),
                "key_factors": load_score.get("key_factors", [])
            }
        }
        
        # Add Rateview market data if available (keep original format)
        if rate_data:
            processed_match["marketData"] = {
                "mileage": rate_data.get("mileage"),
                "reports": rate_data.get("reports"),
                "companies": rate_data.get("companies"),
                "standardDeviation": rate_data.get("standardDeviation"),
                "perMile": rate_data.get("perMile", {}),
                "perTrip": rate_data.get("perTrip", {}),
                "averageFuelSurchargePerMileUsd": rate_data.get("averageFuelSurchargePerMileUsd"),
                "averageFuelSurchargePerTripUsd": rate_data.get("averageFuelSurchargePerTripUsd")
            }
            
            # Add escalation data if available
            if market_data and "escalation" in market_data:
                processed_match["marketData"]["escalation"] = market_data["escalation"]
        elif market_data and "error" in market_data:
            processed_match["marketData"] = {"error": market_data["error"]}
        
        result["processedMatches"].append(processed_match)
    
    return result

def process_freight_data(loads_data: Dict[str, Any], access_token: str) -> Dict[str, Any]:
    """Process freight data and return structured results."""
    return process_loads_and_compare_rates(loads_data, access_token)

def handler(job):
    """
    Runpod serverless handler function.
    
    Args:
        job: Contains the job input with freight data and access token
        
    Returns:
        Processed results with rate comparisons
    """
    job_input = job["input"]
    
    # Validate input
    if not isinstance(job_input, dict):
        return {"error": "Input must be a dictionary"}
    
    # Extract required parameters
    freight_data = job_input.get("freight_data")
    access_token = job_input.get("access_token")
    
    # Validate parameters
    if not freight_data:
        return {"error": "Missing required parameter: freight_data"}
    
    if not access_token:
        return {"error": "Missing required parameter: access_token"}
    
    # Process the data and return results
    try:
        result = process_freight_data(freight_data, access_token)
        return result
    except Exception as e:
        return {"error": f"Processing error: {str(e)}"}

# Start the Runpod serverless function
runpod.serverless.start({"handler": handler})
#################################################################################################################################################################
# import runpod
# import requests
# import json
# from typing import Dict, List, Any, Optional, Union
# from datetime import datetime

# def map_equipment_code_to_rateview(equipment_code: str) -> str:
#     """Maps load search API equipment codes to Rateview API equipment types."""
#     van_codes = ["V", "VA", "VB", "VC", "V2", "VZ", "VH", "VI", "VN", "VG", "VL", "VV", "VM", "VT", "VF", "VR", "VP", "VW"]
#     reefer_codes = ["R", "RA", "R2", "RZ", "RN", "RL", "RM", "RG", "RV", "RP"]
#     flatbed_codes = ["F", "FA", "FT", "FM", "FD", "FR", "FO", "FN", "FS"]
    
#     if equipment_code in van_codes:
#         return "VAN"
#     elif equipment_code in reefer_codes:
#         return "REEFER"
#     elif equipment_code in flatbed_codes:
#         return "FLATBED"
#     else:
#         # Default to FLATBED if unknown
#         return "FLATBED"

# # ==== WEATHER FUNCTIONS ====

# def get_weather_description(code):
#     """Convert WMO weather code to human-readable description."""
#     weather_codes = {
#         0: "Clear sky",
#         1: "Mainly clear",
#         2: "Partly cloudy",
#         3: "Overcast",
#         45: "Fog",
#         48: "Depositing rime fog",
#         51: "Light drizzle",
#         53: "Moderate drizzle",
#         55: "Dense drizzle",
#         56: "Light freezing drizzle",
#         57: "Dense freezing drizzle",
#         61: "Slight rain",
#         63: "Moderate rain",
#         65: "Heavy rain",
#         66: "Light freezing rain",
#         67: "Heavy freezing rain",
#         71: "Slight snow fall",
#         73: "Moderate snow fall",
#         75: "Heavy snow fall",
#         77: "Snow grains",
#         80: "Slight rain showers",
#         81: "Moderate rain showers",
#         82: "Violent rain showers",
#         85: "Slight snow showers",
#         86: "Heavy snow showers",
#         95: "Thunderstorm",
#         96: "Thunderstorm with slight hail",
#         99: "Thunderstorm with heavy hail"
#     }
    
#     return weather_codes.get(code, f"Unknown weather code: {code}")

# def get_weather_data(lat, lon, forecast_days=2):
#     """Get weather forecast data for a specific location using Open-Meteo API."""
#     api_url = "https://api.open-meteo.com/v1/forecast"
    
#     params = {
#         "latitude": lat,
#         "longitude": lon,
#         "hourly": "temperature_2m,precipitation,windspeed_10m,weathercode",
#         "daily": "weathercode,temperature_2m_max,temperature_2m_min,precipitation_sum",
#         "timezone": "auto",
#         "forecast_days": forecast_days
#     }
    
#     try:
#         response = requests.get(api_url, params=params)
        
#         if response.status_code == 200:
#             data = response.json()
            
#             result = {
#                 "location": {
#                     "latitude": data["latitude"],
#                     "longitude": data["longitude"],
#                     "timezone": data["timezone"],
#                     "elevation": data.get("elevation")
#                 },
#                 "hourly_forecast": [],
#                 "daily_forecast": [],
#                 "api_status": "success"
#             }
            
#             # Process hourly forecast
#             hourly_time = data["hourly"]["time"]
#             hourly_temp = data["hourly"]["temperature_2m"]
#             hourly_precip = data["hourly"]["precipitation"]
#             hourly_wind = data["hourly"]["windspeed_10m"]
#             hourly_weather = data["hourly"]["weathercode"]
            
#             for i in range(min(24, len(hourly_time))):  # Limit to first 24 hours
#                 result["hourly_forecast"].append({
#                     "time": hourly_time[i],
#                     "temperature_celsius": hourly_temp[i],
#                     "precipitation_mm": hourly_precip[i],
#                     "wind_speed_kmh": hourly_wind[i],
#                     "weather_code": hourly_weather[i],
#                     "weather_description": get_weather_description(hourly_weather[i])
#                 })
            
#             # Process daily forecast
#             daily_time = data["daily"]["time"]
#             daily_weather = data["daily"]["weathercode"]
#             daily_temp_max = data["daily"]["temperature_2m_max"]
#             daily_temp_min = data["daily"]["temperature_2m_min"]
#             daily_precip = data["daily"]["precipitation_sum"]
            
#             for i in range(len(daily_time)):
#                 result["daily_forecast"].append({
#                     "date": daily_time[i],
#                     "weather_code": daily_weather[i],
#                     "weather_description": get_weather_description(daily_weather[i]),
#                     "max_temperature_celsius": daily_temp_max[i],
#                     "min_temperature_celsius": daily_temp_min[i],
#                     "precipitation_sum_mm": daily_precip[i]
#                 })
            
#             return result
#         else:
#             return {
#                 "error": f"API error: {response.status_code}",
#                 "api_status": "error"
#             }
    
#     except Exception as e:
#         return {
#             "error": f"Exception: {str(e)}",
#             "api_status": "exception"
#         }

# def check_for_hazards(weather_data):
#     """Analyze weather data to identify potential hazards."""
#     if "error" in weather_data:
#         return {"error": weather_data["error"]}
    
#     hazards = []
    
#     # Check hourly forecast for hazards
#     for hour in weather_data["hourly_forecast"]:
#         # Check for severe weather codes
#         weather_code = hour["weather_code"]
#         if weather_code in [71, 73, 75, 77, 85, 86]:  # Snow
#             hazards.append({
#                 "type": "Snow",
#                 "time": hour["time"],
#                 "severity": "high" if weather_code in [75, 86] else "medium"
#             })
#         elif weather_code in [95, 96, 99]:  # Thunderstorms
#             hazards.append({
#                 "type": "Thunderstorm",
#                 "time": hour["time"],
#                 "severity": "high"
#             })
#         elif weather_code in [66, 67]:  # Freezing rain
#             hazards.append({
#                 "type": "Freezing Rain",
#                 "time": hour["time"],
#                 "severity": "high"
#             })
#         elif weather_code in [45, 48]:  # Fog
#             hazards.append({
#                 "type": "Fog",
#                 "time": hour["time"],
#                 "severity": "medium"
#             })
        
#         # Check for heavy precipitation
#         if hour["precipitation_mm"] > 4:
#             hazards.append({
#                 "type": "Heavy Rain",
#                 "time": hour["time"],
#                 "precipitation": hour["precipitation_mm"],
#                 "severity": "medium"
#             })
        
#         # Check for high winds
#         if hour["wind_speed_kmh"] > 30:
#             hazards.append({
#                 "type": "High Winds",
#                 "time": hour["time"],
#                 "wind_speed": hour["wind_speed_kmh"],
#                 "severity": "medium" if hour["wind_speed_kmh"] < 50 else "high"
#             })
    
#     return {
#         "location": weather_data["location"],
#         "hazards": hazards,
#         "hazard_count": len(hazards),
#         "has_severe_hazards": any(h["severity"] == "high" for h in hazards)
#     }

# # NEW: Add function to estimate time delays due to weather
# def estimate_weather_delay(hazards, trip_miles):
#     """
#     Estimate the delay in hours that a driver might experience due to weather conditions.
    
#     Args:
#         hazards: List of weather hazards
#         trip_miles: Total trip distance in miles
        
#     Returns:
#         Dictionary with delay estimates
#     """
#     if not hazards or trip_miles <= 0:
#         return {
#             "estimated_delay_hours": 0,
#             "delay_factors": [],
#             "impact_level": "None"
#         }
    
#     # Define speed reduction factors for different weather conditions
#     # Format: (percentage of normal speed, percentage of route affected)
#     weather_impacts = {
#         "Snow": {
#             "high": (0.4, 0.6),  # 40% of normal speed for 60% of route
#             "medium": (0.6, 0.5)  # 60% of normal speed for 50% of route
#         },
#         "Freezing Rain": {
#             "high": (0.35, 0.5),
#             "medium": (0.5, 0.4)
#         },
#         "Thunderstorm": {
#             "high": (0.5, 0.3),
#             "medium": (0.7, 0.2)
#         },
#         "Fog": {
#             "high": (0.4, 0.4),
#             "medium": (0.6, 0.3)
#         },
#         "Heavy Rain": {
#             "high": (0.6, 0.4),
#             "medium": (0.7, 0.3)
#         },
#         "High Winds": {
#             "high": (0.7, 0.6),
#             "medium": (0.8, 0.4)
#         }
#     }
    
#     # Assume average speed of 55 mph under normal conditions
#     normal_speed_mph = 55
    
#     # Calculate normal trip time in hours
#     normal_trip_time = trip_miles / normal_speed_mph
    
#     # Track total delay and factors
#     total_delay_hours = 0
#     delay_factors = []
    
#     # Process each weather hazard
#     for hazard in hazards:
#         hazard_type = hazard.get("type")
#         severity = hazard.get("severity", "medium")
        
#         if hazard_type in weather_impacts and severity in weather_impacts[hazard_type]:
#             # Get impact parameters for this hazard
#             speed_factor, route_affected = weather_impacts[hazard_type][severity]
            
#             # Calculate affected miles
#             affected_miles = trip_miles * route_affected
            
#             # Calculate normal time for affected segment
#             normal_segment_time = affected_miles / normal_speed_mph
            
#             # Calculate actual time for affected segment with reduced speed
#             actual_segment_time = affected_miles / (normal_speed_mph * speed_factor)
            
#             # Calculate delay for this segment
#             segment_delay = actual_segment_time - normal_segment_time
            
#             # Only count significant delays (more than 15 minutes)
#             if segment_delay > 0.25:
#                 total_delay_hours += segment_delay
#                 delay_factors.append({
#                     "type": hazard_type,
#                     "severity": severity,
#                     "delay_hours": round(segment_delay, 1)
#                 })
    
#     # Determine impact level
#     if total_delay_hours < 0.5:
#         impact_level = "Minimal"
#     elif total_delay_hours < 2:
#         impact_level = "Moderate"
#     elif total_delay_hours < 4:
#         impact_level = "Significant"
#     else:
#         impact_level = "Severe"
    
#     return {
#         "estimated_delay_hours": round(total_delay_hours, 1),
#         "delay_percentage": round((total_delay_hours / normal_trip_time) * 100, 1),
#         "delay_factors": delay_factors,
#         "impact_level": impact_level
#     }

# def get_simple_weather_analysis(origin_weather_data, destination_weather_data, trip_miles):
#     """Create a simple weather analysis summary for drivers."""
    
#     analysis = {
#         "summary": "No weather data available",
#         "risk_level": "Unknown",
#         "risk_score": 0,
#         "hazards": [],
#         "estimated_delay": {
#             "estimated_delay_hours": 0,
#             "delay_factors": [],
#             "impact_level": "None"
#         }
#     }
    
#     # Check if we have valid weather data
#     if not origin_weather_data or not destination_weather_data:
#         return analysis
    
#     origin_hazards = None
#     dest_hazards = None
#     all_hazards = []
    
#     # Process origin weather data
#     if "error" not in origin_weather_data and origin_weather_data.get("api_status") == "success":
#         origin_hazards = check_for_hazards(origin_weather_data)
#         if "error" not in origin_hazards:
#             for hazard in origin_hazards.get("hazards", []):
#                 analysis["hazards"].append(f"Origin: {hazard['type']} ({hazard['severity']} risk)")
#                 all_hazards.append(hazard)
    
#     # Process destination weather data
#     if "error" not in destination_weather_data and destination_weather_data.get("api_status") == "success":
#         dest_hazards = check_for_hazards(destination_weather_data)
#         if "error" not in dest_hazards:
#             for hazard in dest_hazards.get("hazards", []):
#                 analysis["hazards"].append(f"Destination: {hazard['type']} ({hazard['severity']} risk)")
#                 all_hazards.append(hazard)
    
#     # Get first day forecast summaries where available
#     origin_weather = "Unknown"
#     destination_weather = "Unknown"
    
#     if origin_weather_data and "daily_forecast" in origin_weather_data and origin_weather_data["daily_forecast"]:
#         first_day = origin_weather_data["daily_forecast"][0]
#         origin_weather = first_day.get("weather_description", "Unknown")
    
#     if destination_weather_data and "daily_forecast" in destination_weather_data and destination_weather_data["daily_forecast"]:
#         first_day = destination_weather_data["daily_forecast"][0]
#         destination_weather = first_day.get("weather_description", "Unknown")
    
#     # Calculate a risk score from hazards
#     risk_score = 0
#     if origin_hazards and "hazards" in origin_hazards:
#         severe_hazards = [h for h in origin_hazards["hazards"] if h["severity"] == "high"]
#         medium_hazards = [h for h in origin_hazards["hazards"] if h["severity"] == "medium"]
#         risk_score += len(severe_hazards) * 20 + len(medium_hazards) * 10
    
#     if dest_hazards and "hazards" in dest_hazards:
#         severe_hazards = [h for h in dest_hazards["hazards"] if h["severity"] == "high"]
#         medium_hazards = [h for h in dest_hazards["hazards"] if h["severity"] == "medium"]
#         risk_score += len(severe_hazards) * 15 + len(medium_hazards) * 5
    
#     # Cap risk score at 100
#     risk_score = min(risk_score, 100)
    
#     # Determine risk level
#     if risk_score == 0:
#         risk_level = "None"
#     elif risk_score < 25:
#         risk_level = "Low"
#     elif risk_score < 50:
#         risk_level = "Medium"
#     elif risk_score < 75:
#         risk_level = "High"
#     else:
#         risk_level = "Severe"
    
#     # NEW: Estimate delay due to weather conditions
#     delay_info = estimate_weather_delay(all_hazards, trip_miles)
    
#     # Create summary
#     if analysis["hazards"]:
#         hazard_types = set()
#         for hazard in analysis["hazards"]:
#             for h_type in ["Snow", "Thunderstorm", "Freezing Rain", "Fog", "Heavy Rain", "High Winds"]:
#                 if h_type in hazard:
#                     hazard_types.add(h_type)
        
#         hazard_list = ", ".join(sorted(list(hazard_types)))
        
#         # Add delay information to summary
#         if delay_info["estimated_delay_hours"] > 0:
#             delay_hours = delay_info["estimated_delay_hours"]
#             impact = delay_info["impact_level"]
#             analysis["summary"] = f"Weather conditions include {hazard_list}. Expect {impact.lower()} delays of approximately {delay_hours} hours. Origin: {origin_weather}, Destination: {destination_weather}"
#         else:
#             analysis["summary"] = f"Weather conditions include {hazard_list}. No significant delays expected. Origin: {origin_weather}, Destination: {destination_weather}"
#     else:
#         analysis["summary"] = f"No significant weather hazards. Origin: {origin_weather}, Destination: {destination_weather}"
    
#     analysis["risk_level"] = risk_level
#     analysis["risk_score"] = risk_score
#     analysis["estimated_delay"] = delay_info
    
#     return analysis

# def calculate_load_score(rate_comparison, weather_risk_score, deadhead_miles, trip_miles, driver_pay, weather_delay_hours):
#     """Calculate an overall load quality score (0-100)."""
#     # Start with a base score of 50
#     quality_score = 50
#     factors = []
    
#     # Factor 1: Rate comparison (up to +/- 25 points)
#     if isinstance(rate_comparison, (int, float)):
#         # Cap at +/- 30% for scoring purposes
#         capped_difference = max(min(rate_comparison, 30), -30)
#         # Convert to score component (-25 to +25)
#         rate_score = capped_difference * (25/30)
#         quality_score += rate_score
        
#         if rate_comparison > 0:
#             factors.append(f"{round(rate_comparison, 1)}% above market rate")
#         elif rate_comparison < 0:
#             factors.append(f"{abs(round(rate_comparison, 1))}% below market rate")
    
#     # Factor 2: Weather risk (up to -20 points)
#     weather_impact = -(weather_risk_score / 5)
#     quality_score += weather_impact
    
#     if weather_risk_score > 50:
#         factors.append("High weather risk")
#     elif weather_risk_score > 25:
#         factors.append("Moderate weather risk")
    
#     # Factor 3: Weather delay impact (up to -15 points)
#     if weather_delay_hours > 0:
#         # Max penalty is 15 points for 5+ hour delay
#         delay_impact = -min(weather_delay_hours * 3, 15)
#         quality_score += delay_impact
        
#         if weather_delay_hours >= 4:
#             factors.append(f"Severe delay risk ({weather_delay_hours} hrs)")
#         elif weather_delay_hours >= 2:
#             factors.append(f"Significant delay risk ({weather_delay_hours} hrs)")
    
#     # Factor 4: Deadhead impact (up to -15 points)
#     if deadhead_miles > 0 and trip_miles > 0:
#         deadhead_ratio = deadhead_miles / trip_miles
#         deadhead_impact = -min(deadhead_ratio * 30, 15)
#         quality_score += deadhead_impact
        
#         if deadhead_ratio > 0.3:
#             factors.append("High deadhead")
    
#     # Factor 5: Driver pay (up to +15 points)
#     if isinstance(driver_pay, (int, float)) and driver_pay > 0:
#         # Simple tier-based bonus
#         if driver_pay > 500:
#             pay_bonus = 15
#             factors.append("Excellent pay")
#         elif driver_pay > 350:
#             pay_bonus = 10
#             factors.append("Good pay")
#         elif driver_pay > 250:
#             pay_bonus = 5
#         else:
#             pay_bonus = 0
            
#         quality_score += pay_bonus
    
#     # Ensure score is between 0 and 100
#     final_score = round(max(0, min(100, quality_score)))
    
#     # Determine quality category
#     if final_score >= 80:
#         category = "Excellent"
#     elif final_score >= 65:
#         category = "Good"
#     elif final_score >= 45:
#         category = "Average"
#     elif final_score >= 25:
#         category = "Poor"
#     else:
#         category = "Very Poor"
    
#     return {
#         "score": final_score,
#         "category": category,
#         "key_factors": factors[:3]  # Limit to top 3 factors
#     }

# # ==== ORIGINAL FREIGHT RATE FUNCTIONS ====

# def get_broker_rate_per_mile(load: Dict[str, Any]) -> Optional[float]:
#     """Extract broker rate per mile from load data."""
#     # Try estimated rate per mile first
#     if load.get("estimatedRatePerMile", 0) > 0:
#         return load["estimatedRatePerMile"]
    
#     # Try to calculate from total rate and trip length
#     trip_length_miles = load.get("tripLength", {}).get("miles", 0)
#     if trip_length_miles <= 0:
#         return None
    
#     # Check for privateNetworkRateInfo
#     private_rate = load.get("privateNetworkRateInfo", {}).get("bookable", {}).get("rate", {}).get("rateUsd", 0)
#     if private_rate > 0:
#         return private_rate / trip_length_miles
    
#     # Check for loadBoardRateInfo
#     load_board_rate = load.get("loadBoardRateInfo", {}).get("nonBookable", {}).get("rateUsd", 0)
#     if load_board_rate > 0:
#         return load_board_rate / trip_length_miles
    
#     return None

# def get_total_load_amount(load: Dict[str, Any]) -> Optional[float]:
#     """Extract total load amount from load data."""
#     # Check for privateNetworkRateInfo
#     private_rate = load.get("privateNetworkRateInfo", {}).get("bookable", {}).get("rate", {}).get("rateUsd", 0)
#     if private_rate > 0:
#         return private_rate
    
#     # Check for loadBoardRateInfo
#     load_board_rate = load.get("loadBoardRateInfo", {}).get("nonBookable", {}).get("rateUsd", 0)
#     if load_board_rate > 0:
#         return load_board_rate
    
#     return None

# def calculate_driver_pay(load: Dict[str, Any]) -> Dict[str, Union[float, str]]:
#     """
#     Calculate driver pay as 25% of total load amount. 
#     If load amount not available, calculate as rate per mile * trip length.
#     """
#     trip_length_miles = load.get("tripLength", {}).get("miles", 0)
    
#     # Try to get total load amount
#     total_load_amount = get_total_load_amount(load)
    
#     if total_load_amount and total_load_amount > 0:
#         # Driver pay is 25% of total load amount
#         driver_pay = total_load_amount * 0.25
#         source = "percentage_of_total"
#     else:
#         # Get rate per mile
#         rate_per_mile = get_broker_rate_per_mile(load)
        
#         if rate_per_mile and rate_per_mile > 0 and trip_length_miles > 0:
#             # Calculate total from rate per mile and trip length
#             total_calculated = rate_per_mile * trip_length_miles
#             driver_pay = total_calculated * 0.25
#             source = "calculated_from_rate_per_mile"
#         else:
#             # Not enough data to calculate
#             return {
#                 "amount": "Not Available",
#                 "calculation_method": "insufficient_data"
#             }
    
#     return {
#         "amount": round(driver_pay, 2),
#         "calculation_method": source
#     }

# def get_rate_comparison(load_rate: Optional[float], market_rate: Optional[float]) -> Dict[str, Union[float, str]]:
#     """Calculate percentage difference between broker rate and market rate."""
#     if load_rate is None or load_rate <= 0 or market_rate is None or market_rate <= 0:
#         return {
#             "broker_rate_per_mile": "Not Available" if load_rate is None or load_rate <= 0 else load_rate,
#             "market_rate_per_mile": "Not Available" if market_rate is None or market_rate <= 0 else market_rate,
#             "difference_percentage": "N/A",
#             "comparison": "Rate comparison not possible"
#         }
    
#     difference_percentage = ((load_rate - market_rate) / market_rate) * 100
    
#     comparison = (
#         f"{abs(round(difference_percentage, 2))}% above market rate"
#         if difference_percentage > 0
#         else f"{abs(round(difference_percentage, 2))}% below market rate"
#         if difference_percentage < 0
#         else "At market rate"
#     )
    
#     return {
#         "broker_rate_per_mile": load_rate,
#         "market_rate_per_mile": market_rate,
#         "difference_percentage": round(difference_percentage, 2),
#         "comparison": comparison
#     }

# def call_rateview_api(origin: Dict[str, str], destination: Dict[str, str], 
#                      equipment: str, access_token: str) -> Dict[str, Any]:
#     """Call the Rateview API to get market rate information."""
#     base_url = "https://analytics.api.staging.dat.com/linehaulrates"
#     endpoint = "/v1/lookups"
#     url = base_url + endpoint
    
#     # Format the payload
#     payload = [{
#         "origin": {
#             "city": origin.get("city", ""),
#             "stateOrProvince": origin.get("stateProv", "")
#         },
#         "destination": {
#             "city": destination.get("city", ""),
#             "stateOrProvince": destination.get("stateProv", "")
#         },
#         "rateType": "SPOT",
#         "equipment": equipment,
#         "includeMyRate": True,
#         "targetEscalation": {
#             "escalationType": "BEST_FIT"
#         }
#     }]
    
#     headers = {
#         "Content-Type": "application/json",
#         "Authorization": f"Bearer {access_token}"
#     }
    
#     try:
#         response = requests.post(url, headers=headers, data=json.dumps(payload))
#         if response.status_code in (200, 201):
#             return response.json()
#         else:
#             return {"error": f"API Error: {response.status_code}", "detail": response.text}
#     except Exception as e:
#         return {"error": f"Exception: {str(e)}"}

# def process_loads_and_compare_rates(loads_data: Dict[str, Any], access_token: str) -> Dict[str, Any]:
#     """
#     Process loads data, call Rateview API, and calculate rate comparisons.
#     Now includes weather data, delay estimates, and load quality score.
#     """
#     result = {
#         "matchCounts": loads_data.get("matchCounts", {}),
#         "processedMatches": []
#     }
    
#     matches = loads_data.get("matches", [])
    
#     for match in matches:
#         match_id = match.get("matchId", "")
        
#         # Extract origin and destination
#         matching_asset_info = match.get("matchingAssetInfo", {})
#         origin = matching_asset_info.get("origin", {})
#         destination_place = matching_asset_info.get("destination", {}).get("place", {})
        
#         # Skip if missing critical data
#         if not origin or not destination_place:
#             continue
        
#         # Extract equipment type and map to Rateview format
#         equipment_code = matching_asset_info.get("equipmentType", "")
#         rateview_equipment = map_equipment_code_to_rateview(equipment_code)
        
#         # Get trip miles
#         trip_miles = match.get("tripLength", {}).get("miles", 0)
        
#         # Get broker rate per mile
#         broker_rate_per_mile = get_broker_rate_per_mile(match)
        
#         # Calculate driver pay
#         driver_pay = calculate_driver_pay(match)
        
#         # === Get weather data if coordinates are available
#         origin_weather_data = None
#         destination_weather_data = None
        
#         if origin.get("latitude") and origin.get("longitude"):
#             origin_weather_data = get_weather_data(origin["latitude"], origin["longitude"])
        
#         if destination_place.get("latitude") and destination_place.get("longitude"):
#             destination_weather_data = get_weather_data(destination_place["latitude"], destination_place["longitude"])
        
#         # Get simplified weather analysis with delay estimation
#         weather_analysis = get_simple_weather_analysis(origin_weather_data, destination_weather_data, trip_miles)
        
#         # Call Rateview API
#         rateview_response = call_rateview_api(origin, destination_place, rateview_equipment, access_token)
        
#         # Extract market rate per mile
#         market_rate_per_mile = None
#         market_data = None
#         rate_data = None
        
#         # Process rateview response
#         try:
#             # Check for API errors
#             if "error" in rateview_response:
#                 market_data = {"error": rateview_response.get("error")}
#             else:
#                 # Get the first rate response
#                 rate_responses = rateview_response.get("rateResponses", [])
#                 if rate_responses and len(rate_responses) > 0:
#                     response_obj = rate_responses[0].get("response", {})
                    
#                     # Extract rate data
#                     if "rate" in response_obj:
#                         rate_data = response_obj["rate"]
                        
#                         # Extract market rate per mile if available
#                         if "perMile" in rate_data and "rateUsd" in rate_data["perMile"]:
#                             market_rate_per_mile = rate_data["perMile"]["rateUsd"]
                    
#                     # Store complete market data
#                     market_data = response_obj
#         except Exception as e:
#             market_data = {"error": f"Error processing rate data: {str(e)}"}
        
#         # Calculate rate comparison
#         comparison = get_rate_comparison(broker_rate_per_mile, market_rate_per_mile)
        
#         # Calculate load quality score with weather delay factor
#         load_score = calculate_load_score(
#             comparison.get("difference_percentage") if comparison.get("difference_percentage") != "N/A" else 0,
#             weather_analysis.get("risk_score", 0),
#             match.get("originDeadheadMiles", {}).get("miles", 0),
#             trip_miles,
#             driver_pay.get("amount") if isinstance(driver_pay.get("amount"), (int, float)) else 0,
#             weather_analysis.get("estimated_delay", {}).get("estimated_delay_hours", 0)
#         )
        
#         # Build processed match data - KEEP ORIGINAL FORMAT WITH ADDITIONS
#         processed_match = {
#             "matchId": match_id,
#             "origin": {
#                 "city": origin.get("city", ""),
#                 "state": origin.get("stateProv", "")
#             },
#             "destination": {
#                 "city": destination_place.get("city", ""),
#                 "state": destination_place.get("stateProv", "")
#             },
#             "equipmentType": {
#                 "code": equipment_code,
#                 "rateviewType": rateview_equipment
#             },
#             "tripMiles": trip_miles,
#             "rateComparison": comparison,
#             "driver_pay": driver_pay,
            
#             # Weather info with delay estimates
#             "weatherInfo": {
#                 "summary": weather_analysis.get("summary", "Weather data not available"),
#                 "risk_level": weather_analysis.get("risk_level", "Unknown"),
#                 "risk_score": weather_analysis.get("risk_score", 0),
#                 "hazards": weather_analysis.get("hazards", []),
#                 "estimated_delay": weather_analysis.get("estimated_delay", {
#                     "estimated_delay_hours": 0,
#                     "delay_factors": [],
#                     "impact_level": "None"
#                 })
#             },
            
#             # Load quality score
#             "loadQuality": {
#                 "score": load_score.get("score", 0),
#                 "category": load_score.get("category", "Unknown"),
#                 "key_factors": load_score.get("key_factors", [])
#             }
#         }
        
#         # Add Rateview market data if available (keep original format)
#         if rate_data:
#             processed_match["marketData"] = {
#                 "mileage": rate_data.get("mileage"),
#                 "reports": rate_data.get("reports"),
#                 "companies": rate_data.get("companies"),
#                 "standardDeviation": rate_data.get("standardDeviation"),
#                 "perMile": rate_data.get("perMile", {}),
#                 "perTrip": rate_data.get("perTrip", {}),
#                 "averageFuelSurchargePerMileUsd": rate_data.get("averageFuelSurchargePerMileUsd"),
#                 "averageFuelSurchargePerTripUsd": rate_data.get("averageFuelSurchargePerTripUsd")
#             }
            
#             # Add escalation data if available
#             if market_data and "escalation" in market_data:
#                 processed_match["marketData"]["escalation"] = market_data["escalation"]
#         elif market_data and "error" in market_data:
#             processed_match["marketData"] = {"error": market_data["error"]}
        
#         result["processedMatches"].append(processed_match)
    
#     return result

# def process_freight_data(loads_data: Dict[str, Any], access_token: str) -> Dict[str, Any]:
#     """Process freight data and return structured results."""
#     return process_loads_and_compare_rates(loads_data, access_token)

# def handler(job):
#     """Runpod serverless handler function."""
#     job_input = job["input"]
    
#     # Validate input
#     if not isinstance(job_input, dict):
#         return {"error": "Input must be a dictionary"}
    
#     # Extract required parameters
#     freight_data = job_input.get("freight_data")
#     access_token = job_input.get("access_token")
    
#     # Validate parameters
#     if not freight_data:
#         return {"error": "Missing required parameter: freight_data"}
    
#     if not access_token:
#         return {"error": "Missing required parameter: access_token"}
    
#     # Process the data and return results
#     try:
#         result = process_freight_data(freight_data, access_token)
#         return result
#     except Exception as e:
#         return {"error": f"Processing error: {str(e)}"}

# # Start the Runpod serverless function
# runpod.serverless.start({"handler": handler})
