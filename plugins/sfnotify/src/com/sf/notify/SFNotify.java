package com.sf.notify;

import azkaban.alert.Alerter;
import azkaban.executor.ExecutableFlow;
import azkaban.sla.SlaOption;
import azkaban.utils.Props;
import com.google.common.base.Joiner;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

public class SFNotify implements Alerter {
  private static final Logger log = Logger.getLogger(SFNotify.class);
  private static final String agentId = "2";

  private Notify notify;
  private boolean alertSuccess, alertError, alertSla;
  private String[] emails = new String[]{};

  public SFNotify(Props props) {
    this.notify = new Notify(props.getString("sfnotify.url"));
    this.alertSuccess = props.getBoolean("sfnotify.success", false);
    this.alertError = props.getBoolean("sfnotify.error", true);
    this.alertSla = props.getBoolean("sfnotify.sla", true);
    String emailList = props.getString("sfnotify.emails", "");
    if (emailList != null && !emailList.equals("")) {
      emails = emailList.split(",");
    }
  }

  @Override
  public void alertOnSuccess(ExecutableFlow exflow) throws Exception {
    if (!alertSuccess) {
      return;
    }
    sendNotify(
      String.format(
        "Job [project:%s, flow:%s, execId:%d] sucessed!",
        exflow.getProjectName(),
        exflow.getFlowId(),
        exflow.getExecutionId()
      )
    );
  }

  @Override
  public void alertOnError(ExecutableFlow exflow, String... extraReasons) throws Exception {
    if (!alertError) {
      return;
    }
    sendNotify(
      String.format(
        "Job [project:%s, flow:%s, execId:%d] failed!",
        exflow.getProjectName(),
        exflow.getFlowId(),
        exflow.getExecutionId()
      )
    );
  }

  @Override
  public void alertOnFirstError(ExecutableFlow exflow) throws Exception {
    if (!alertError) {
      return;
    }
    sendNotify(
      String.format(
        "Job [project:%s, flow:%s, execId:%d] first failed!",
        exflow.getProjectName(),
        exflow.getFlowId(),
        exflow.getExecutionId()
      )
    );
  }

  @Override
  public void alertOnSla(SlaOption slaOption, String slaMessage) throws Exception {
    if (!alertSla) {
      return;
    }
    slaMessage = slaMessage.replace("</br>", "\n");
    sendNotify(slaMessage);
  }

  public void sendNotify(String content) {
    log.info("sending msg " + content);
    String title = String.format("notify: %s", "azkaban");
    content = String.format("%s - %s", new DateTime().toString(), content);
    notify.send(new Notify.Wechat(title, content, "0", agentId), true);
    if (emails.length > 0) {
      notify.send(
        new Notify.Email(
          Joiner.on(';').join(emails),
          title,
          content,
          "plain"
        ),
        true
      );
    }
  }
}
