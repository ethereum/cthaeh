from cthaeh.models import Topic, Log
from cthaeh.tools.factories import TopicFactory, LogFactory, LogTopicFactory


def test_single_topic(session):
    topic = TopicFactory()

    session.add(topic)
    session.commit()

    topic_from_db = session.query(Topic).filter(
        Topic.topic == topic.topic,
    ).one()

    assert topic_from_db.topic == topic.topic


def test_log_without_topics(session):
    log = LogFactory()

    session.add(log)
    session.commit()

    log_from_db = session.query(Log).filter(
        Log.id == log.id,
    ).one()

    assert log_from_db.id == log.id


def test_log_with_single_topic(session):
    topic = TopicFactory()
    log = LogFactory()
    log_topic = LogTopicFactory(log=log, topic=topic, idx=0)

    session.add_all((log, topic, log_topic))
    session.commit()

    log_from_db = session.query(Log).filter(
        Log.id == log.id,
    ).one()

    assert log_from_db.id == log.id
    assert len(log_from_db.topics) == 1


def test_log_with_multiple_topics(session):
    topic_a, topic_b, topic_c = TopicFactory.create_batch(3)
    log = LogFactory()
    log_topic_0 = LogTopicFactory(topic=topic_b, log=log, idx=0)
    log_topic_1 = LogTopicFactory(topic=topic_a, log=log, idx=1)
    log_topic_2 = LogTopicFactory(topic=topic_c, log=log, idx=2)

    session.add_all((log, topic_a, topic_b, topic_c, log_topic_0, log_topic_1, log_topic_2))
    session.commit()

    log_from_db = session.query(Log).filter(
        Log.id == log.id,
    ).one()

    assert log_from_db.id == log.id
    assert len(log_from_db.topics) == 3
    assert log.topics[0].topic == topic_b.topic
    assert log.topics[1].topic == topic_a.topic
    assert log.topics[2].topic == topic_c.topic
