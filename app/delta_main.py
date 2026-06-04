import logging
from config import r, STOCKS, EXCHANGE
from StockAnalyser import Delta_analysis
from ws_producer import start_producer
from delta_consumer import start_consumer
from delta_graph import delta_graph


def run():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(threadName)s] %(message)s',
    )

    stock = f"{EXCHANGE}:{STOCKS[0]}"
    logging.info(f"[MAIN] starting live delta analysis for {stock}")

    r.flushall()
    r.set('end', 'false')

    instance = Delta_analysis()
    producer_thread = start_producer(stock)
    consumer_thread = start_consumer(stock, instance)

    try:
        delta_graph(instance)  # blocks on Qt event loop
    except KeyboardInterrupt:
        logging.info("[MAIN] interrupted")
    finally:
        r.set('end', 'true')
        producer_thread.join(timeout=5)
        consumer_thread.join(timeout=5)
        logging.info("[MAIN] exited cleanly")


if __name__ == "__main__":
    run()
