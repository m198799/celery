import fire
import logging

from celery.utils.log import get_task_logger
from celery import group, chord, chain
from tasks import tap, add, add_rand_sleep, xmulti, multi, done, sum_array

logger = get_task_logger(__name__)
logger.setLevel(logging.INFO)


def exp_task_signature():
    add_rand_sleep.s(1).delay(2)


def exp_task_callback():
    add_rand_sleep.apply_async((2, 1), link=multi.s(3))


def exp_group_then_chord():
    """
    """
    job = group([
        add_rand_sleep.s(1, 2),
        add_rand_sleep.s(2, 2),
        add_rand_sleep.s(3, 2),
        add_rand_sleep.s(4, 2),
        add_rand_sleep.s(5, 2),
        add_rand_sleep.s(6, 2),
    ])

    chord(job)(xmulti.s())

    # (job | xmulti.s()).delay()


def exp_group_link():
    """
    """
    job = group([
        add.s(1, 2),
        add.s(2, 2),
        add.s(3, 2),
        add.s(4, 2),
        add.s(5, 2),
        add.s(6, 2),
    ])

    job.link(multi.s())

    job.apply_async()


def exp_chunk():
    arg1 = range(1, 20)
    logger.info("123")
    add_rand_sleep.chunks(zip(arg1, range(1, 20)), 3).apply_async()


def exp_chunk_group():
    arg1 = range(1, 20)
    g = add_rand_sleep.chunks(zip(arg1, range(1, 20)), 3).group()
    chord(g)(tap.s())


def exp_chunk_link():
    """

    类似问题 https://stackoverflow.com/questions/17214479/how-to-call-a-task-at-the-end-of-the-execution-of-each-chunk-in-celery
    """
    arg1 = range(1, 20)
    # ck = add_rand_sleep.chunks(zip(arg1, range(1, 20)), 3)
    # ck.flatten_links(tap)
    # ck.apply_async()


def exp_chain_group_chord():
    chord(group(
        chain(add_rand_sleep.s(1, 1), multi.s(1)),
        chain(add_rand_sleep.s(2, 2), multi.s(2)),
        chain(add_rand_sleep.s(3, 3), multi.s(3)),
        chain(add_rand_sleep.s(4, 4), multi.s(4)),
    ))(chain(tap.s(), sum_array.s(), tap.s()))


def exp_chain_group_chord2():
    """
    https://github.com/celery/celery/issues/6197#issuecomment-824116739
    """
    logger.info("==== hello =====")
    print("==== hello =====")
    s = chain(chord(group(
        chain(add_rand_sleep.s(1, 1), multi.s(1)),
        chain(add_rand_sleep.s(2, 2), multi.s(2)),
        chain(add_rand_sleep.s(3, 3), multi.s(3)),
        chain(add_rand_sleep.s(4, 4), multi.s(4)),
    ), tap.s()), sum_array.s(), tap.s())()
    s.get()
    print("==== run done ====")


def exp_chain_append():
    c = chain(add_rand_sleep.s(1, 2), add_rand_sleep.s(3))
    c |= add_rand_sleep.s(4)
    c |= add_rand_sleep.s(5)
    print(c.apply().get())


if __name__ == '__main__':
    fire.Fire({
        "exp1": exp_group_then_chord,
        "exp2": exp_group_link,
        "exp3": exp_task_signature,
        "exp4": exp_task_callback,
        "exp5": exp_chunk,
        "exp6": exp_chunk_group,
        "exp7": exp_chunk_link,
        "exp8": exp_chain_group_chord,
        "exp9": exp_chain_append,
        "exp10": exp_chain_group_chord2,
    })
