// +build rabbitmqintegration

package rabbitmq

import (
	"fmt"
	"github.com/linkerd/linkerd2/testutil"
	"github.com/streadway/amqp"
	"os"
	"testing"
	"time"
)

var TestHelper *testutil.TestHelper

func TestMain(m *testing.M) {
	TestHelper = testutil.NewTestHelper()
	os.Exit(m.Run())
}

type mqResources struct {
	queueName string
	messageBody string
	expectedMessageCount int
}

var defaultConnStr = "amqp://guest:guest@localhost:5672/"


func validateMQResources(actual string, expected string, message string) error {
	if actual != expected {
		return fmt.Errorf("Expected %s '%s', got '%s'", message, expected, actual)
	}
	return nil
}

func RabbitMQAMQPConnection() (*amqp.Connection, error)  {
	conn, err := amqp.Dial(defaultConnStr)
	if err != nil {
		return nil, fmt.Errorf("Could not connect to rabbitMQ %s", err)
	}
	return conn, nil
}

func TestRabbitMQWithQueuesAndMessage(t *testing.T) {
	qn := &mqResources{
		queueName: "MQTestQueue",
		messageBody: "This is a test message",
		expectedMessageCount: 1,
	}
	/*out, err := TestHelper.LinkerdRun("inject", "--manual", "testdata/rabbitmq.yaml")
	if err != nil {
		testutil.AnnotatedFatalf(t, "'linkerd inject' command failed", err)
	}*/

	conn, err := RabbitMQAMQPConnection()
	if err != nil {
		fmt.Errorf("could not create RabbitMQ connection %s", err)
	}


	t.Run("Connect to Rabbitmq and create a queue", func(t *testing.T){
		timeout := 40 * time.Second
		err := TestHelper.RetryFor(timeout, func() error {

			if err := RabbitMQCreateQueue(conn, qn); err != nil {
				testutil.AnnotatedFatal(t, "Error connect to RabbitMQ and create a queue")
			}
			return nil

		})
		if err != nil {
			testutil.AnnotatedFatal(t, fmt.Sprintf("timed-out connecting to MQ to create a queue (%s)", timeout), err)
		}
	})
	t.Run("Publish a message to RabbitMQ", func(t *testing.T){
		timeout := 40 * time.Second
		err := TestHelper.RetryFor(timeout, func() error{
			if err := RabbitMQPublishMessage(conn, qn); err != nil {
				testutil.AnnotatedFatal(t, fmt.Sprintf("Error publish message to RabbitMQ"), err)
			}
			return nil
		})
		if err != nil {
			testutil.AnnotatedFatal(t, fmt.Sprintf("timed-out connecting to MQ to publish a message (%s)", timeout), err)
		}
	})
	t.Run("Consume a message from RabbitMQ", func(t *testing.T){
		timeout := 40 * time.Second
		err := TestHelper.RetryFor(timeout, func() error{
			if err := RabbitMQPublishMessage(conn, qn); err != nil {
				testutil.AnnotatedFatal(t, fmt.Sprintf("Error consuming message from RabbitMQ"), err)
			}
			return nil
		})
		if err != nil {
			testutil.AnnotatedFatal(t, fmt.Sprintf("timed-out connecting to MQ to consume a message (%s)", timeout), err)
		}
	})


}

func RabbitMQCreateQueue(conn *amqp.Connection, m *mqResources) error {
	ch, err := conn.Channel()
	if err != nil {
		fmt.Errorf("fould not open channel %s", err)
	}

	q, err := ch.QueueDeclare(m.queueName, false, false, false, false, nil)
	if err != nil {
		fmt.Errorf("fould not create RabbitMQ queue %s", err)
	}

	if err := validateMQResources( m.queueName, q.Name, "queue name"); err != nil {
		return fmt.Errorf("failed to create queue %s", m.queueName)
	}
	return nil

}

func  RabbitMQPublishMessage(conn *amqp.Connection, m *mqResources) error {
	ch, err := conn.Channel()
	if err != nil {
		fmt.Errorf("fould not open channel %s", err)
	}
	err = ch.Publish(
		"",
		m.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body: []byte(m.messageBody),
		})
	if err != nil {
		fmt.Errorf("failed to publish message %s", err)
	}
	result, _:= ch.QueueInspect(m.queueName)
	fmt.Printf("%d and %d\n",result.Messages, m.expectedMessageCount)
	if result.Messages != m.expectedMessageCount {
		return fmt.Errorf("expected messagecount %d got messageCount %d", m.expectedMessageCount, result.Messages)
	}
	return nil
}

func RabbitMQConsumeMessage(conn *amqp.Connection, m *mqResources) error{
	ch, err := conn.Channel()
	if err != nil {
		fmt.Errorf("fould not open channel %s", err)
	}
	msgs, err := ch.Consume(
		m.queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		fmt.Errorf("failed to consume message %s", err)
	}
	for d := range msgs {
		validateMQResources( string(d.Body) , m.messageBody, "message body")
	}
	return nil
}
