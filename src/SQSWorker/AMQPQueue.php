<?php

namespace SQSWorker;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use Exception;

class AMQPQueue extends AbstractQueue
{
  protected $connection;
  protected $queue;
  protected $channel;

  public function __construct(array $args = [])
  {
    $keys = ['connection', 'queue'];

    foreach($keys as $key) {
      if(!isset($args[$key])) {
        throw new Exception("Missing required argument $key");
      }
    }

    if( !($args['connection'] instanceof AMQPStreamConnection) ) {
      throw new Exception("Connection must be an instance of AMQPStreamConnection");
    }

    $this->connection = $args['connection'];
    $this->queue      = $args['queue'];
    $this->channel    = $this->connection->channel();

    $this->channel->queue_declare($this->queue, false, true, false, false);
    $this->channel->basic_qos(null, 1, null);
    $this->channel->basic_consume($this->queue, '', false, false, false, false, [$this, 'processMessage']);
  }

  public function loop()
  {
    return count($this->channel->callbacks);
  }

  public function receive()
  {
    $this->channel->wait();
  }

  public function processMessage($msg)
  {
    $this->respond([
      'id'   => $msg->delivery_info['consumer_tag'],
      'tag'  => $msg->delivery_info['delivery_tag'],
      'body' => $msg->body,
    ]);
  }

  public function ack($tag)
  {
    $this->channel->basic_ack($tag);
  }

  public function nack($tag)
  {
    $this->channel->basic_nack($tag);
  }

  public function close()
  {
    $this->channel->close();
    $this->connection->close();
  }

  public function __destruct()
  {
    $this->close();
  }
}
