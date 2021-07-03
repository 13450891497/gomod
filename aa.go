package main

import (
	"AdxDc/app/rokmq"
	"AdxDc/controllers"
	_ "AdxDc/routers"
	"github.com/astaxie/beego"
	"strings"
)

func main() {
	//MQ消费
	topics := strings.Split(beego.AppConfig.String("topic"), ",")
	ConsumeWithOrderly(topics...)

	// 启动rpc
	//go controllers.NewGRpcServer()
	beego.Run()

}

// MQ消费：
func ConsumeWithOrderly(topics ...string) {
	for _, v := range topics {
		switch v {
		case rokmq.Topic:
			// mq消费 历史统计
			mq := controllers.RocketmqController{}

			consumerOption := rokmq.ConsumerOption{
				GroupId: rokmq.GroupId,
				Topic:   rokmq.Topic,
				Tags:    []string{rokmq.TagAction, rokmq.TagTRACK},
				Model:   rokmq.CoCurrentlyConsumer, // 异步消费
				//Model: rokmq.OrderlyConsumer, // 严格顺序消费
			}
			go rokmq.TcpConsumeWithOrderlyV2(consumerOption, mq.RocketMqCallback)
		case rokmq.TopicView:
			// view 消费
			mq := controllers.RocketmqController{}
			consumerOption := rokmq.ConsumerOption{
				GroupId: rokmq.GroupId02,
				Topic:   rokmq.TopicView,
				Tags:    []string{rokmq.TagView},
				Model:   rokmq.CoCurrentlyConsumer,
			}
			go rokmq.TcpConsumeWithOrderlyV2(consumerOption, mq.RocketMqCallback)
		case rokmq.TopicUserRecall:
			//mq消费 召回老用户
			userRecall := controllers.UserRecallController{}
			consumerOption := rokmq.ConsumerOption{
				GroupId: rokmq.GroupId03,
				Topic:   rokmq.TopicUserRecall,
				Tags:    []string{rokmq.TagUserRecall},
				Model:   rokmq.CoCurrentlyConsumer,
			}
			go rokmq.TcpConsumeWithOrderlyV2(consumerOption, userRecall.RocketMqCallback)
		case rokmq.TopicClick:
			// Click 消费
			mq := controllers.RocketmqController{}
			consumerOption := rokmq.ConsumerOption{
				GroupId: rokmq.GroupId04,
				Topic:   rokmq.TopicClick,
				Tags:    []string{rokmq.TagClick},
				Model:   rokmq.CoCurrentlyConsumer,
			}
			go rokmq.TcpConsumeWithOrderlyV2(consumerOption, mq.RocketMqCallback)
		case rokmq.TopicSem:
			// Sem 消费
			sem := controllers.SemController{}
			consumerOption := rokmq.ConsumerOption{
				GroupId: rokmq.GroupId05,
				Topic:   rokmq.TopicSem,
				Tags:    []string{rokmq.TagSem},
				Model:   rokmq.CoCurrentlyConsumer,
			}
			go rokmq.TcpConsumeWithOrderlyV2(consumerOption, sem.RocketMqCallback)
		case rokmq.TopicDepthCall:
			// 深度回传消费
			depth := controllers.DepthCallController{}
			consumerOption := rokmq.ConsumerOption{
				GroupId: rokmq.GroupId06,
				Topic:   rokmq.TopicDepthCall,
				Tags:    []string{rokmq.TagDepthCall},
				Model:   rokmq.CoCurrentlyConsumer,
			}
			go rokmq.TcpConsumeWithOrderlyV2(consumerOption, depth.RocketMqCallback)
		}
	}

}
