<?php

namespace SQSWorker;

use Aws\Sqs\SqsClient;
use Exception;

class SQSQueue extends AbstractQueue
{
  protected $client;
  protected $queue_url;

  public function __construct(array $args = [])
  {
    $keys = ['client', 'queue_url'];

    foreach($keys as $key) {
      if(!isset($args[$key])) {
        throw new Exception("Missing required argument $key");
      }
    }

    if( !($args['client'] instanceof SqsClient) ) {
      throw new Exception("Client must be an instance of SqsClient");
    }

    $this->client    = $args['client'];
    $this->queue_url = $args['queue_url'];
  }

  public function loop()
  {
    return true;
  }

  public function receive()
  {
    $result = $this->client->receiveMessage([
      'QueueUrl'            => $this->queue_url,
      'MaxNumberOfMessages' => 1,
      'WaitTimeSeconds'     => 20,
    ]);

    if(!($result instanceof \Guzzle\Service\Resource\Model)) {
      throw new Exception("Received unknown response of type " . get_class($result));
    }

    if(!isset($result['Messages']) or !is_array($result['Messages']) or !count($result['Messages'])) {
      $this->respond([]);
    }

    $message = $result['Messages'][0];

    $this->respond([
      'id'   => $message['MessageId'],
      'tag'  => $message['ReceiptHandle'],
      'body' => $message['Body'],
    ]);
  }

  public function ack($tag)
  {
    $this->client->deleteMessage([
      'QueueUrl'      => $this->queue_url,
      'ReceiptHandle' => $tag,
    ]);
  }

  public function nack($tag)
  {
  }

  public function close()
  {
  }
}
