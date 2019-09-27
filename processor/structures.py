from enum import Enum
import datetime
import unittest


class OrderType(Enum):
    """Order type. BUY or SELL."""
    BUY = 0
    SELL = 1

class OrderMap(object):
    """Price vs Amount map.\n
    Type of price can be any, and it can be retrived from getter price_type.\n
    Immutable.
    """
    def __init__(self, price_type: type):
        self._orders = dict()
        self._price_type = price_type

    @property
    def price_type(self):
        """Return type object which price of orders represented as.\n
        ex. if prices are represented as int type, it returns \"<class 'int'>\"\n
        Type can be anything from int to object depends on exchanges' implementation and coin specification,
        though it's usualy int or float.
        """
        return self._price_type

    def __getitem__(self, key):
        # Check if keys' type is correct
        if not isinstance(key, self._price_type):
            # type.__name__ will return type's name like "int"
            raise TypeError('key must be "%s" type' % self._price_type.__name__)
        # If object is comparable, check if key is positive number
        cmp_func = getattr(key, '__le__', None)
        if cmp_func is not None and callable(cmp_func) and key <= 0:
            raise KeyError('key must be an positive number')
        
        if key not in self._orders:
            return 0
        else:
            return self._orders[key]

class Board(object):
    """Represents board state with time.\n
    Board state is, which is a combination of sell orders and buy orders.
    Board state can not be accessed externally before taking a snapshot of one.
    """

    def __init__(self, price_type):
        self._sells = dict()
        self._buys = dict()
        self._price_type = price_type

    @property
    def price_type(self):
        """See #OrderMap.price_type.\n
        This function returns the same value as sells/buys.price_type.
        """
        return self._price_type

    def progress(self, time_delta):
        """Change board state as progressing/rewinding time by as much as given on a \"time_delta\" parameter.\n
        time_delta can be \"int\" as well as \"datetime.timedelta\".
        If int value is given, time_delta nanosecond time progresses/rewinds.
        If datetime.timedelta value is given, time as much as timedelta represents progresses/rewinds.\n
        More detail about progressing/rewinding time is explained at set_time function.
        """
        if isinstance(time_delta, datetime.timedelta):
            # Type of time_delta is datetime.timedelta
            pass
        elif isinstance(time_delta, int):
            # Or int
            pass
        else:
            # Otherwise, it is not supported
            raise TypeError('"int" or "datetime.timedelta" type is supported as a "time_delta" parameter.')

        pass

    def set_time(self, abs_time :datetime.datetime):
        """Change board state as setting absolute time to what is given on a \"abs_time\" parameter.\n
        After calling this function, board state will be set as if it is exactly at abs_time and passed it,
        but before abs_time + (1 nanosecond).\n
        ex. set_time(datetime.datetime(2019, 2, 7, 10, 43, 30, 567890)) will set this board state to
        exactly when at 2 Feb 2019 10:43:30.567890 but before 10:43:30.567891 .
        This means changes having timestamp of 10:43:30.567890 will be applied to the state, but not at 567891.
        """
        pass

    def tick(self):
        """Change board state as progress time by a time unit, which is 1 nanosecond."""
        self.progress(1)

    def take_snapshot(self):
        """Take snapshot of current board state.\n
        Both sell/buy orders that this instance of board has
        will be copied in newly created BoardState instance.
        The instance won't be changed after the creation
        even if a state of board which snapshot taken from changes."""
        pass

    def __add__(self, delta):
        """Perform self + \"delta\". An result is a new BoardState instance,
        which represents this board state but delta applied to it."""
        pass

    def __sub__(self, subtrahend):
        """Perform self - \"subtrahend\". An result is a new BoardState instance,
        which represents delta(change) of board state from subtrahend."""
        pass


class BoardState(object):
    """Snapshot of an board state. Immutable."""
    def __init__(self, price_type: type):
        self._price_type = price_type
        self._sells = OrderMap(price_type)
        self._buys = OrderMap(price_type)

    @property
    def price_type(self):
        """See #Board.price_type."""
        return self._price_type

    @property
    def sells(self):
        """Return sell order price vs amount map."""
        return self._sells

    @property
    def buys(self):
        """Return buy order price vs amount map."""
        return self._buys

    def __getitem__(self, key):
        """Return order list of given OrderType"""
        if not isinstance(key, OrderType):
            raise TypeError('key must be "OrderType" type')
        if key == OrderType.SELL:
            return self._sells
        elif key == OrderType.BUY:
            return self._buys
        else:
            raise KeyError('Unknown key: %s' % key)

class TestStructures(unittest.TestCase):
    def test_board_snapshot(self):
        board = BoardState(int)

        # buys/sells could be accessed from outside
        board.sells
        # Modifying buys/sells from outside of a class should fail
        with self.assertRaises(AttributeError):
            board.sells = None
        # TypeError should be raised if type of "key" is not OrderType in __getitem__
        with self.assertRaises(TypeError):
            board[None]
        # board[OrderType.SELL/BUY] is an alternative to board.buys/board.sells for each
        self.assertEqual(board[OrderType.BUY], board.buys)
        self.assertEqual(board[OrderType.SELL], board.sells)
    
    def test_order_list(self):
        ol = OrderMap(int)

        # KeyError is raised when a non positive number is given as price
        with self.assertRaises(KeyError):
            ol[0]
        # TypeError is raised when type of "key" is not float
        with self.assertRaises(TypeError):
            ol[None]
        # Amount of inititialized order price must be reported as 0
        self.assertEqual(ol[1], 0)
        

if __name__ == '__main__':
    unittest.main()
