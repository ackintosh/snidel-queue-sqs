<?php
namespace Ackintosh\Snidel\Queue\Sqs;

use Ackintosh\Snidel\Config;
use Ackintosh\Snidel\Result\Result as SnidelResult;
use Ackintosh\Snidel\Result\Formatter;
use Ackintosh\Snidel\Result\QueueInterface;
use Aws\Sqs\SqsClient;

class Result implements QueueInterface
{
    /** @var \Aws\Sqs\SqsClient */
    private $sqsClient;

    /** @var string */
    private $queueUrl;

    /** @var int */
    private $dequeuedCount = 0;

    /**
     * @param   \Ackintosh\Snidel\Config
     */
    public function __construct(Config $config)
    {
        $this->config = $config;
        $this->sqsClient = $this->createSqsClient();

        $queueName = sprintf('result_%s_%d', gethostname(), $config->get('ownerPid'));
        // can only include alphanumeric characters, hyphens, or underscores
        $queueName = preg_replace('/[^a-zA-Z0-9_-]*/', '', $queueName);

        $result = $this->sqsClient->createQueue(
            array('QueueName' => $queueName)
        );

        $this->queueUrl = $result->get('QueueUrl');
    }

    private function createSqsClient()
    {
        return SqsClient::factory(array(
            'key'    => $this->config->get('aws-key'),
            'secret' => $this->config->get('aws-secret'),
            'region' => $this->config->get('aws-region'),
        ));
    }

    /**
     * @return  \Ackintosh\Snidel\Result\Result
     */
    public function enqueue(SnidelResult $result)
    {
        $serialized = Formatter::serialize($result);

        $this->sqsClient->sendMessage(
            array(
                'QueueUrl'    => $this->queueUrl,
                'MessageBody' => base64_encode($serialized),
            )
        );
    }

    /**
     * @return  \Ackintosh\Snidel\Result\Result
     */
    public function dequeue()
    {
        while (true) {
            $r = $this->sqsClient->receiveMessage(
                array(
                    'QueueUrl'            => $this->queueUrl,
                    'MaxNumberOfMessages' => 1,
                    'WaitTimeSeconds'     => 20,
                )
            );

            if (isset($r['Messages'])) {
                break;
            }
        }

        $serialized = base64_decode($r['Messages'][0]['Body']);

        $this->sqsClient->deleteMessage(
            array(
                'QueueUrl'      => $this->queueUrl,
                'ReceiptHandle' => $r['Messages'][0]['ReceiptHandle'],
            )
        );

        $this->dequeuedCount++;

        return Formatter::unserialize($serialized);
    }

    /**
     * @return  int
     */
    public function dequeuedCount()
    {
        return $this->dequeuedCount;
    }
}
