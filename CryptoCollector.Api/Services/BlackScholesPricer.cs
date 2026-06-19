namespace CryptoCollector.Api.Services;

public sealed class BlackScholesPricer
{
    public decimal Price(
        bool isCall,
        decimal spot,
        decimal strike,
        decimal timeToExpiryYears,
        decimal volatility,
        decimal riskFreeRate = 0m)
    {
        if (spot <= 0 || strike <= 0)
        {
            return 0m;
        }

        if (timeToExpiryYears <= 0 || volatility <= 0)
        {
            return IntrinsicValue(isCall, spot, strike);
        }

        var s = (double)spot;
        var k = (double)strike;
        var t = (double)timeToExpiryYears;
        var sigma = (double)volatility;
        var r = (double)riskFreeRate;
        var sqrtT = Math.Sqrt(t);
        var variance = sigma * sqrtT;
        if (variance <= 0)
        {
            return IntrinsicValue(isCall, spot, strike);
        }

        var d1 = (Math.Log(s / k) + (r + 0.5d * sigma * sigma) * t) / variance;
        var d2 = d1 - variance;
        var discount = Math.Exp(-r * t);

        var price = isCall
            ? (s * NormalCdf(d1)) - (k * discount * NormalCdf(d2))
            : (k * discount * NormalCdf(-d2)) - (s * NormalCdf(-d1));

        return decimal.CreateChecked(price);
    }

    public decimal NormalizeVolatility(decimal? iv)
    {
        if (iv is null || iv <= 0)
        {
            return 0m;
        }

        var value = iv.Value;
        if (value > 3m)
        {
            value /= 100m;
        }

        return Math.Clamp(value, 0.0001m, 5m);
    }

    public decimal IntrinsicValue(bool isCall, decimal spot, decimal strike) =>
        isCall
            ? Math.Max(spot - strike, 0m)
            : Math.Max(strike - spot, 0m);

    private static double NormalCdf(double value) => 0.5d * (1d + Erf(value / Math.Sqrt(2d)));

    private static double Erf(double value)
    {
        var sign = Math.Sign(value);
        var absolute = Math.Abs(value);
        var a1 = 0.254829592d;
        var a2 = -0.284496736d;
        var a3 = 1.421413741d;
        var a4 = -1.453152027d;
        var a5 = 1.061405429d;
        var p = 0.3275911d;
        var t = 1d / (1d + p * absolute);
        var y = 1d - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.Exp(-absolute * absolute);
        return sign * y;
    }
}
