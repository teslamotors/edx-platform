""" receivers of course_published and library_updated events in order to trigger indexing task """


import logging
from datetime import datetime
from functools import wraps

from django.core.cache import cache
from django.dispatch import receiver
from pytz import UTC

from cms.djangoapps.contentstore.courseware_index import (
    CourseAboutSearchIndexer,
    CoursewareSearchIndexer,
    LibrarySearchIndexer
)
from cms.djangoapps.contentstore.proctoring import register_special_exams
from common.djangoapps.track.event_transaction_utils import get_event_transaction_id, get_event_transaction_type
from common.djangoapps.util.module_utils import yield_dynamic_descriptor_descendants
from lms.djangoapps.grades.api import task_compute_all_grades_for_course
from openedx.core.djangoapps.credit.signals import on_course_publish
from openedx.core.djangoapps.content.learning_sequences.api import key_supports_outlines
from openedx.core.lib.gating import api as gating_api
from xmodule.modulestore.django import SignalHandler, modulestore

from .signals import GRADING_POLICY_CHANGED

log = logging.getLogger(__name__)

GRADING_POLICY_COUNTDOWN_SECONDS = 25 * 60 # to avoid timing out on rabbitmq


def locked(expiry_seconds, key):  # lint-amnesty, pylint: disable=missing-function-docstring
    def task_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = '{}-{}'.format(func.__name__, kwargs[key])
            if cache.add(cache_key, "true", expiry_seconds):
                log.info('Locking task in cache with key: %s for %s seconds', cache_key, expiry_seconds)
                return func(*args, **kwargs)
            else:
                log.info('Task with key %s already exists in cache', cache_key)
        return wrapper
    return task_decorator


@receiver(GRADING_POLICY_CHANGED)
@locked(expiry_seconds=GRADING_POLICY_COUNTDOWN_SECONDS, key='course_key')
def handle_grading_policy_changed(sender, **kwargs):
    # pylint: disable=unused-argument
    """
    Receives signal and kicks off celery task to recalculate grades
    """
    kwargs = {
        'course_key': str(kwargs.get('course_key')),
        'grading_policy_hash': str(kwargs.get('grading_policy_hash')),
        'event_transaction_id': str(get_event_transaction_id()),
        'event_transaction_type': str(get_event_transaction_type()),
    }
    result = task_compute_all_grades_for_course.apply_async(kwargs=kwargs, countdown=GRADING_POLICY_COUNTDOWN_SECONDS)
    log.info("Grades: Created {task_name}[{task_id}] with arguments {kwargs}".format(
        task_name=task_compute_all_grades_for_course.name,
        task_id=result.task_id,
        kwargs=kwargs,
    ))
