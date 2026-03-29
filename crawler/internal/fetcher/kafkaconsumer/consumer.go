package kafkaconsumer

import (
	"context"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)


type Handler func(context.Context, *kgo.Record) error

func Consume(
	ctx context.Context, client *kgo.Client,
	handler Handler, workerCount int,
) error {

	msgCh := make(chan *kgo.Record)
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
	   defer close(msgCh)
	   for {
		  fetches := client.PollFetches(ctx)
		  if fetches.IsClientClosed() {
		    return nil
		  }

		  if errs := fetches.Errors(); len(errs) > 0 {
			// usually errs contain context.Canceled if the context was closed
			if ctx.Err() != nil {
			   return ctx.Err()
			}

			for _, err := range errs {
			   log.Printf("fetch error for topic %s partition %d: %v", err.Topic, err.Partition, err.Err)
			}

		  }

		  if fetches.Empty() {
			continue
		  }

		  iter := fetches.RecordIter()

		  for !iter.Done() {
			rec := iter.Next()
			select {
			  case <-ctx.Done():
				return ctx.Err()
			  case msgCh <- rec:
			}
		  }
	   }
	})

	if workerCount < 1 {
		workerCount = 1
	}

	for i := 0; i < workerCount; i++ {
		g.Go(func() error {
			for msg := range msgCh {
				if err := handler(ctx, msg); err != nil {
					log.Printf("handler error : %v", err)
				}

				if err := client.CommitRecords(ctx, msg); err != nil {
					log.Printf("commit error : %v", err)
				}
			}

			return nil
		})
	}

	return g.Wait()


}