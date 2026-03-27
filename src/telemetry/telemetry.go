package telemetry

import (
	"context"
	"log"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const payloadCarrierKey = "_otel"

var textMapPropagator = propagation.TraceContext{}

func Init(ctx context.Context, serviceName string, attrs ...attribute.KeyValue) func(context.Context) error {
	if os.Getenv("AEGEAN_DISABLE_TRACING") == "1" {
		log.Printf("telemetry disabled: AEGEAN_DISABLE_TRACING=1")
		return func(context.Context) error { return nil }
	}

	outputPath := os.Getenv("AEGEAN_OTEL_FILE_PATH")
	if outputPath == "" {
		outputPath = "/tmp/otel-traces.json"
	}

	f, err := os.Create(outputPath)
	if err != nil {
		log.Printf("telemetry disabled: create trace file %s: %v", outputPath, err)
		return func(context.Context) error { return nil }
	}
	exporter, err := stdouttrace.New(
		stdouttrace.WithWriter(f),
	)
	if err != nil {
		log.Printf("telemetry disabled: create exporter: %v", err)
		_ = f.Close()
		return func(context.Context) error { return nil }
	}

	resAttrs := append([]attribute.KeyValue{
		semconv.ServiceName(serviceName),
	}, attrs...)
	res, err := resource.New(ctx, resource.WithAttributes(resAttrs...))
	if err != nil {
		log.Printf("telemetry disabled: create resource: %v", err)
		return func(context.Context) error { return nil }
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(textMapPropagator)
	log.Printf("telemetry enabled: service=%s output=%s", serviceName, outputPath)
	return func(ctx context.Context) error {
		defer f.Close()
		return provider.Shutdown(ctx)
	}
}

func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

func StartSpanFromPayload(payload map[string]any, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	ctx := context.Background()
	if payload != nil {
		ctx = ExtractContext(ctx, payload)
	}
	ctx, span := Tracer("aegean").Start(ctx, name, trace.WithAttributes(attrs...))
	return ctx, span
}

func StartLocalSpanFromPayload(payload map[string]any, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	ctx := context.Background()
	if payload != nil {
		ctx = ExtractContext(ctx, payload)
	}
	ctx, span := Tracer("aegean").Start(ctx, name, trace.WithAttributes(attrs...))
	return ctx, span
}

func InjectContext(ctx context.Context, payload map[string]any) {
	if payload == nil {
		return
	}
	carrier := payloadCarrier(payload)
	textMapPropagator.Inject(ctx, carrier)
}

func CopyContext(dst, src map[string]any) {
	if dst == nil || src == nil {
		return
	}
	ctx := ExtractContext(context.Background(), src)
	InjectContext(ctx, dst)
}

func ExtractContext(ctx context.Context, payload map[string]any) context.Context {
	if payload == nil {
		return ctx
	}
	return textMapPropagator.Extract(ctx, payloadCarrier(payload))
}

func payloadCarrier(payload map[string]any) propagation.MapCarrier {
	if payload == nil {
		return propagation.MapCarrier{}
	}
	raw, ok := payload[payloadCarrierKey]
	if !ok {
		carrier := propagation.MapCarrier{}
		payload[payloadCarrierKey] = map[string]string(carrier)
		return carrier
	}
	switch typed := raw.(type) {
	case map[string]string:
		return propagation.MapCarrier(typed)
	case map[string]any:
		carrier := propagation.MapCarrier{}
		for key, value := range typed {
			if str, ok := value.(string); ok {
				carrier[key] = str
			}
		}
		payload[payloadCarrierKey] = map[string]string(carrier)
		return carrier
	default:
		carrier := propagation.MapCarrier{}
		payload[payloadCarrierKey] = map[string]string(carrier)
		return carrier
	}
}
