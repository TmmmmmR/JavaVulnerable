package com.fundoonotes.messagesservice;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.fundoonotes.exception.FNException;
import com.fundoonotes.searchservice.ElasticSyncService;
import com.fundoonotes.searchservice.IESService;
import com.fundoonotes.utilityservice.Email;
import com.fundoonotes.utilityservice.MailService;

public class MessageConsumer<T> implements MessageListener
{

   @Autowired
   private IESService esService;

   @Autowired
   private MailService mailService;

   @Autowired
   private ElasticSyncService elasticSyncService;

   private final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

   public MessageConsumer()
   {
      //Default Constructor
   }

   @SuppressWarnings("unchecked")
   @Override
   public void onMessage(Message msg)
   {
      logger.info("Inside onMessage - fetching Object from activeMQ");

      try {
         ObjectMessage objectMessage = (ObjectMessage) msg;
         JmsDto<T> jmsDto = (JmsDto<T>) objectMessage.getObject();

         switch (jmsDto.getOperation()) {
         case SAVE:
            esService.save(jmsDto.getObject());
            break;
         case UPDATE:
            esService.update(jmsDto.getObject());
            break;
         case DELETE:
            esService.deleteById(jmsDto.getObject());
            break;
         default:
            Email email = (Email) jmsDto.getObject();
            mailService.send(email.getSubject(), email.getBody(), email.getTo());
            break;
         }
      }
      catch (Exception e) {
         try {
            e.printStackTrace();
            ObjectMessage objectMessage = (ObjectMessage) msg;
            JmsDto<T> jmsDto = (JmsDto<T>) objectMessage.getObject();
            elasticSyncService.add(jmsDto);
         } catch (IllegalArgumentException | IllegalAccessException | JMSException | FNException e1) {
         }
      }
      logger.info("fetched and called ElasticService");
   }

}
