using SkiaSharp;

namespace CryptoCollector.Api.Services;

public sealed class PositionPnlChartRenderer
{
    public byte[] Render(PositionPnlChart chart)
    {
        using var surface = SKSurface.Create(new SKImageInfo(1200, 800));
        var canvas = surface.Canvas;
        canvas.Clear(SKColors.White);

        const float left = 110f;
        const float right = 40f;
        const float top = 50f;
        const float bottom = 90f;
        const float width = 1200f - left - right;
        const float height = 800f - top - bottom;

        var xMin = chart.Prices.Min();
        var xMax = chart.Prices.Max();
        var yMin = Math.Min(0m, chart.ExpiryPnl.Min());
        var yMax = Math.Max(0m, chart.ExpiryPnl.Max());
        if (chart.CurrentPnl.Count > 0)
        {
            yMin = Math.Min(yMin, chart.CurrentPnl.Min());
            yMax = Math.Max(yMax, chart.CurrentPnl.Max());
        }

        if (yMin == yMax)
        {
            yMin -= 1m;
            yMax += 1m;
        }

        float MapX(decimal x) => left + (float)((x - xMin) / (xMax - xMin)) * width;
        float MapY(decimal y) => top + height - (float)((y - yMin) / (yMax - yMin)) * height;

        using var gridPaint = new SKPaint { Color = new SKColor(230, 230, 230), StrokeWidth = 1, IsAntialias = true };
        using var axisPaint = new SKPaint { Color = SKColors.Black, StrokeWidth = 2, IsAntialias = true };
        using var zeroPaint = new SKPaint { Color = new SKColor(180, 60, 60), StrokeWidth = 2, IsAntialias = true };
        using var spotPathEffect = SKPathEffect.CreateDash([8, 8], 0);
        using var spotPaint = new SKPaint { Color = new SKColor(50, 100, 200), StrokeWidth = 2, IsAntialias = true, PathEffect = spotPathEffect };
        using var expiryLinePaint = new SKPaint { Color = new SKColor(0, 120, 90), StrokeWidth = 4, IsAntialias = true, Style = SKPaintStyle.Stroke };
        using var currentLinePaint = new SKPaint { Color = new SKColor(214, 110, 0), StrokeWidth = 4, IsAntialias = true, Style = SKPaintStyle.Stroke };
        using var textPaint = new SKPaint { Color = SKColors.Black, IsAntialias = true };
        using var titlePaint = new SKPaint { Color = SKColors.Black, IsAntialias = true };
        using var smallTextPaint = new SKPaint { Color = new SKColor(70, 70, 70), IsAntialias = true };
        using var textFont = new SKFont { Size = 20 };
        using var titleFont = new SKFont { Size = 28, Embolden = true };
        using var smallTextFont = new SKFont { Size = 18 };

        DrawGrid(canvas, textPaint, textFont, gridPaint, xMin, xMax, yMin, yMax, left, top, width, height, MapX, MapY);
        canvas.DrawLine(left, top + height, left + width, top + height, axisPaint);
        canvas.DrawLine(left, top, left, top + height, axisPaint);
        canvas.DrawLine(left, MapY(0), left + width, MapY(0), zeroPaint);
        canvas.DrawLine(MapX(chart.CurrentSpot), top, MapX(chart.CurrentSpot), top + height, spotPaint);

        using var expiryPath = new SKPath();
        expiryPath.MoveTo(MapX(chart.Prices[0]), MapY(chart.ExpiryPnl[0]));
        for (var index = 1; index < chart.Prices.Count; index++)
        {
            expiryPath.LineTo(MapX(chart.Prices[index]), MapY(chart.ExpiryPnl[index]));
        }

        canvas.DrawPath(expiryPath, expiryLinePaint);

        if (chart.CurrentPnl.Count == chart.Prices.Count)
        {
            using var currentPath = new SKPath();
            currentPath.MoveTo(MapX(chart.Prices[0]), MapY(chart.CurrentPnl[0]));
            for (var index = 1; index < chart.Prices.Count; index++)
            {
                currentPath.LineTo(MapX(chart.Prices[index]), MapY(chart.CurrentPnl[index]));
            }

            canvas.DrawPath(currentPath, currentLinePaint);
        }

        canvas.DrawText($"Expiry Payoff ({chart.DisplayCurrency})", left, 30, SKTextAlign.Left, titleFont, titlePaint);
        canvas.DrawText($"Current spot: {chart.CurrentSpot:0.##}", left + width - 230, 30, SKTextAlign.Left, textFont, textPaint);
        canvas.DrawText($"As of: {chart.AsOfUtc:dd MMM yyyy HH:mm:ss} UTC", left, top + height + 62, SKTextAlign.Left, smallTextFont, smallTextPaint);
        DrawLegend(canvas, left + width - 270, top + 14, expiryLinePaint, currentLinePaint, spotPaint, smallTextPaint, smallTextFont);

        using var image = surface.Snapshot();
        using var data = image.Encode(SKEncodedImageFormat.Png, 100);
        return data.ToArray();
    }

    private static void DrawGrid(
        SKCanvas canvas,
        SKPaint textPaint,
        SKFont textFont,
        SKPaint gridPaint,
        decimal xMin,
        decimal xMax,
        decimal yMin,
        decimal yMax,
        float left,
        float top,
        float width,
        float height,
        Func<decimal, float> mapX,
        Func<decimal, float> mapY)
    {
        for (var tick = 0; tick <= 5; tick++)
        {
            var xValue = xMin + (xMax - xMin) * tick / 5m;
            var x = mapX(xValue);
            canvas.DrawLine(x, top, x, top + height, gridPaint);
            canvas.DrawText($"{xValue:0}", x - 25, top + height + 30, SKTextAlign.Left, textFont, textPaint);

            var yValue = yMin + (yMax - yMin) * tick / 5m;
            var y = mapY(yValue);
            canvas.DrawLine(left, y, left + width, y, gridPaint);
            canvas.DrawText(FormatCompact(yValue), 10, y + 7, SKTextAlign.Left, textFont, textPaint);
        }
    }

    private static string FormatCompact(decimal value)
    {
        var absolute = Math.Abs(value);
        return absolute switch
        {
            >= 1_000_000m => $"{value / 1_000_000m:0.##}M",
            >= 1_000m => $"{value / 1_000m:0.##}K",
            _ => $"{value:0}"
        };
    }

    private static void DrawLegend(
        SKCanvas canvas,
        float x,
        float y,
        SKPaint expiryLinePaint,
        SKPaint currentLinePaint,
        SKPaint spotPaint,
        SKPaint textPaint,
        SKFont textFont)
    {
        DrawLegendItem(canvas, x, y, expiryLinePaint, "Expiry");
        DrawLegendItem(canvas, x, y + 24, currentLinePaint, "Current");
        DrawLegendItem(canvas, x, y + 48, spotPaint, "Spot");

        void DrawLegendItem(SKCanvas targetCanvas, float itemX, float itemY, SKPaint paint, string label)
        {
            targetCanvas.DrawLine(itemX, itemY, itemX + 32, itemY, paint);
            targetCanvas.DrawText(label, itemX + 42, itemY + 6, SKTextAlign.Left, textFont, textPaint);
        }
    }
}

public sealed record PositionPnlChart(
    string DisplayCurrency,
    decimal CurrentSpot,
    IReadOnlyList<decimal> Prices,
    IReadOnlyList<decimal> ExpiryPnl,
    IReadOnlyList<decimal> CurrentPnl,
    DateTime AsOfUtc);
