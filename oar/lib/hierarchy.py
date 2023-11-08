# coding: utf-8
"""
This module defines function to handle resources Hierarchy.
In oar, resources are organized as a `nested sets`_.

.. _nested sets: https://en.wikipedia.org/wiki/Nested_set_model
"""

from procset import ProcSet


class Hierarchy(object):
    # TODO extract hierarchy from ressources table

    def __init__(
        self,
        hy=None,
        hy_rid=None,
    ):
        if hy_rid:
            self.hy = {}
            for hy_label, hy_level_roids in hy_rid.items():
                self.hy[hy_label] = [ProcSet(*ids) for k, ids in hy_level_roids.items()]
        else:
            if hy:
                self.hy = hy
            else:
                raise Exception("Hierarchy description must be provided")


def keep_no_empty_scat_bks(itvs, itvss_ref):
    """
    Filter :class:`ProcSet` from itvss_ref that has no element in `itvs`

    :param ProcSet itvs: ProcSet of resources
    :param [ProcSet] itvs_ref: Array of procsets
    :return: \
         [:class:`ProcSet`]; Elements in `itvss_ref` that have non-null intersection with `itvs`

    Examples:
        >>> keep_no_empty_scat_bks(ProcSet((1, 32)), [ProcSet((1, 8)), ProcSet(30, 33), ProcSet(34, 38)])
            [ProcSet((1, 8)), ProcSet(30, 33)]
    """
    lr = len(itvss_ref)
    i = 0
    r_itvss = []

    while i < lr:
        x = itvss_ref[i]
        if len(x & itvs) != 0:
            r_itvss.append(x)
        i += 1
    return r_itvss


def extract_n_scattered_block_itv(itvs1, itvs_ref, n):
    """
    Try to take `n` resources from hierarchy level `itvs_ref`.
    Only takes resources from resource for which all their sub-resources are available.

    :param ProcSet itvs1: \
        class:`ProcSet` of available resources of the current hierarchy (`itvs_ref`)
    :param [ProcSet] itvs_ref: \
        An hierarchy level
    :param Integer n: \
        Array containing the number of resources to extract from itvs1
    :return: \
        A :class:`ProcSet` containing the intervalset that can be extracted from itvs1,
        or an empty :class:`ProcSet` if the request cannot be fullfilled

    Examples:
        >>> extract_n_scattered_block_itv(ProcSet((1, 32)), [ProcSet((1, 16)), ProcSet((17, 32))], 2)
            ProcSet((1, 32))

        >>> extract_n_scattered_block_itv(ProcSet((1, 32)), [ProcSet((1, 16)), ProcSet((17, 32))], 1)
            ProcSet((1, 16))

        >>> # Take the second interval because the (1, 16) is not full
        >>> extract_n_scattered_block_itv(ProcSet((2, 32)), [ProcSet((1, 16)), ProcSet((17, 32))], 1)
            ProcSet((17, 32))

        >>> extract_n_scattered_block_itv(
        ...    ProcSet(*[(2, 16), (17, 31)]),
        ...    [ ProcSet((1, 16)), ProcSet((17, 32)) ],
        ...    1)
            ProcSet()
    """
    lr = len(itvs_ref)
    i = 0

    itvs = ProcSet()

    # While we still need to collect, or the hierarchy level is empty
    while (n > 0) and (i < lr):
        x = itvs_ref[i]
        y = itvs1 & x
        # Check that all the resources contained in itvs_ref[i] are free
        if x == y:
            itvs = itvs | y
            n -= 1
        i += 1

    if n == 0:
        return itvs
    else:
        # Not enough resources in the hierarchy, the request cannot be fullfilled
        return ProcSet()


def find_resource_hierarchies_scattered(itvs, hy, rqts):
    """
    According to a resources set `itvs`, find a resources set compatible with the request in `rqts`.

    :param ProcSet itvs: A :class:`ProcSet` of available resources
    :param [ProcSet] hy: The specified hierarchy levels
    :param [Integer] rqts: \
        Array containing the number of resources needed by level of hierarchy
    :return:
        A :class:`ProcSet` containing resources compatible with the request, or empty if the request could not be satisfied.

    Examples:
        >>> # Create two levels of hierarchy
        >>> h0 = [ProcSet(*y) for y in [[(1, 16)], [(17, 32)]]]
            [ProcSet((1, 16)), ProcSet((17, 32))]
        >>> h1 = [ProcSet(*y) for y in [[(1, 8)], [(9, 16)], [(17, 24)], [(18, 32)]]]
            [ProcSet((1, 8)), ProcSet((9, 16)), ProcSet((17, 24)), ProcSet((18, 32))]
        >>> available_resources = ProcSet(*[(1, 32)])
            ProcSet((1, 32))
        >>> # Simple test with one hierarchy level, and take one resources from it
        >>> find_resource_hierarchies_scattered (available_resources, [h0], [1])
            ProcSet((1, 16))
        >>> # Take one resources from h0, and two from h1
        >>> find_resource_hierarchies_scattered (available_resources, [h0, h1], [1, 2])
            ProcSet((1, 16))
        >>> # Take two from h0, and one h1
        >>> find_resource_hierarchies_scattered (available_resources, [h0, h1], [2, 1])
            ProcSet((1, 8), (17, 24))
        >>> # Impossible request
        >>> find_resource_hierarchies_scattered (available_resources, [h0, h1], [1, 3])
            ProcSet()
    """

    l_hy = len(hy)
    #    print "find itvs: ", itvs, rqts[0]
    if l_hy == 1:
        return extract_n_scattered_block_itv(itvs, hy[0], rqts[0])
    else:
        # Call to recursive function
        return find_resource_n_h(itvs, hy, rqts, hy[0], 0, l_hy)


def find_resource_n_h(itvs, hy, rqts, top, h, h_bottom):
    """
    Recursive function collecting resources from each hierarchy level.

    :param itvs: A :class:`ProcSet` of available resources
    :param [ProcSet] hy: The specified hierarchy levels
    :param [Integer] rqts: \
        Array containing the number of resources needed by level of hierarchy
    :param top: \
        Current level of hierarchy to consider
    :param h: \
        Current level of hierarchy to consider
    :param h_bottom: \
        Last level of hierarchy (used to stop recursive call)
    :return:
        A :class:`ProcSet` containing resources compatible with the request, or empty if the request could not be satisfied.

    """
    # potential available blocks
    # Filter hierarchy levels that has no available block anyway
    avail_bks = keep_no_empty_scat_bks(itvs, top)
    l_avail_bks = len(avail_bks)

    if l_avail_bks < rqts[h]:
        # not enough scattered blocks
        return ProcSet()
    else:
        if h == h_bottom - 2:
            # reach last level hierarchy of requested resource
            # iter on top and find rqts[h-1] block
            itvs_acc = ProcSet()
            i = 0
            nb_r = 0
            while (i < l_avail_bks) and (nb_r != rqts[h]):  # need
                # print avail_bks[i], "*", hy[h+1]
                # TODO test cost of [] filtering .....
                avail_sub_bks = [
                    (avail_bks[i] & x) for x in hy[h + 1] if len(avail_bks[i] & x) != 0
                ]
                # print avail_sub_bks
                # print "--------------------------------------"
                r = extract_n_scattered_block_itv(itvs, avail_sub_bks, rqts[h + 1])
                # r = []
                if len(r) != 0:
                    # win for this top_block
                    itvs_acc = itvs_acc | r
                    nb_r += 1
                i += 1
            if nb_r == rqts[h]:
                return itvs_acc
            else:
                return ProcSet()

        else:
            # intermediate hierarchy level
            # iter on available_bk
            itvs_acc = ProcSet()
            i = 0
            nb_r = 0
            while (i < l_avail_bks) and (nb_r != rqts[h]):
                # Current picked level
                level = avail_bks[i]
                # Select children of this level to propagate it into the recursive call
                children = [sub for sub in hy[h + 1] if sub.issubset(level)]
                r = find_resource_n_h(itvs, hy, rqts, children, h + 1, h_bottom)
                # print("R: {}".format(r))
                if len(r) != 0:
                    # win for this top_block
                    itvs_acc = itvs_acc | r
                    nb_r += 1
                i += 1
            if nb_r == rqts[h]:
                return itvs_acc
            else:
                return ProcSet()


# def G(Y):
#    if one h level:
#        extract
#    else:
#        F(X)
#
#
# def F(X):
#    avail_bks
#    return if not enough bks
#    if bottom-1:
#       take (
#        intersec (avail_kbs[i] h+1)
#        extract )
#    else:
#        take ( find(new X new level) )


# h0 = [[(1, 16)],[(17, 32)]]
# h1 = [[(1,8)],[(9,16)],[(17,24)],[(25,32)]]
# h2 = [[(1,4)], [(5,8)], [(8,12)], [(13,16)], [(17,20)],[(21,24)],[(25,28)],[(29,32)]]

# find_resource_hierarchies_scattered ([(1, 32)], [h0,h1,h2], [2,1,1])

# find_resource_hierarchies_scattered ([(1, 32)], [h0], [2])
# [(1, 16), (17, 32)]
# find_resource_hierarchies_scattered ([(10, 32)], [h0], [1])
# [(17, 32)]
# find_resource_hierarchies_scattered ([(10, 32)], [h0], [2])
# []
# find_resource_hierarchies_scattered ([(1, 32)], [h0,h1], [2,1])
# - : Interval.interval list = [{b = 1; e = 16}; {b = 17; e = 32}]
